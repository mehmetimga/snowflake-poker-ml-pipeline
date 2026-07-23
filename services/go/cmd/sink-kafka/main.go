package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/kafkaio"
	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/sink"
)

func main() {
	brokers := flag.String(
		"bootstrap-servers",
		envDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		"comma-separated Kafka brokers",
	)
	groupID := flag.String(
		"group-id",
		envDefault("KAFKA_SNOWFLAKE_SINK_GROUP_ID", "poker-snowflake-sink-synthetic-v1"),
		"isolated Kafka consumer group",
	)
	writerURL := flag.String(
		"writer-url",
		envDefault("SNOWFLAKE_EVENT_WRITER_URL", "http://127.0.0.1:8091"),
		"private Snowflake event-writer sidecar URL",
	)
	securityProtocol := flag.String(
		"security-protocol",
		envDefault("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
		"PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL",
	)
	saslMechanism := flag.String(
		"sasl-mechanism",
		envDefault("KAFKA_SASL_MECHANISM", "PLAIN"),
		"Kafka SASL mechanism",
	)
	allowedTenants := flag.String(
		"allowed-tenants",
		envDefault("SINK_ALLOWED_TENANTS", "demo"),
		"comma-separated tenant allowlist",
	)
	buildVersion := flag.String(
		"build-version",
		envDefault("SINK_SERVICE_BUILD_VERSION", "dev"),
		"immutable service image or source build version",
	)
	metricsListen := flag.String(
		"metrics-listen",
		envDefault("SINK_METRICS_LISTEN", "127.0.0.1:9094"),
		"private health/Prometheus listen address; empty disables",
	)
	fromBeginning := flag.Bool(
		"from-beginning",
		envBool("SINK_FROM_BEGINNING", false),
		"start at the earliest offset when no group offset exists",
	)
	checkDependenciesOnly := flag.Bool(
		"check-dependencies-only",
		false,
		"verify Kafka and Snowflake writer readiness without consuming",
	)
	maxPollRecords := flag.Int(
		"max-poll-records",
		100,
		"maximum records returned from one poll",
	)
	maxRecords := flag.Int(
		"max-records",
		0,
		"stop after persisting and committing this many records; zero runs continuously",
	)
	requestTimeout := flag.Duration(
		"request-timeout",
		30*time.Second,
		"Kafka startup, writer request, and HTTP shutdown timeout",
	)
	flag.Parse()

	if !strings.Contains(strings.ToLower(*groupID), "synthetic") {
		log.Fatal("POKER_SINK currently accepts only an isolated synthetic consumer group")
	}
	if strings.TrimSpace(*buildVersion) == "" {
		log.Fatal("SINK_SERVICE_BUILD_VERSION or --build-version is required")
	}

	routes := sink.CanonicalSyntheticRoutes()
	topics := make([]string, 0, len(routes))
	for _, route := range routes {
		topics = append(topics, route.Topic)
	}
	httpClient := &http.Client{Timeout: *requestTimeout}
	persister, err := sink.NewHTTPPersister(*writerURL, httpClient)
	if err != nil {
		log.Fatalf("configure Snowflake writer: %v", err)
	}
	kafkaClient, err := kafkaio.NewClient(kafkaio.Config{
		Brokers: strings.Split(*brokers, ","), ClientID: "poker-snowflake-sink-v1",
		GroupID: *groupID, InputTopics: topics,
		SecurityProtocol: *securityProtocol, SASLMechanism: *saslMechanism,
		Username:      os.Getenv("KAFKA_SASL_USERNAME"),
		Password:      os.Getenv("KAFKA_SASL_PASSWORD"),
		FromBeginning: *fromBeginning, MaxPollRecords: *maxPollRecords,
	})
	if err != nil {
		log.Fatalf("configure Kafka: %v", err)
	}
	defer kafkaClient.Close()
	processor, err := sink.NewProcessor(sink.Config{
		Routes: routes, AllowedTenants: splitNonEmpty(*allowedTenants),
		ServiceBuildVersion: *buildVersion,
	}, persister, kafkaClient)
	if err != nil {
		log.Fatalf("configure sink processor: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	startupCtx, startupCancel := context.WithTimeout(ctx, *requestTimeout)
	if err := kafkaClient.Ping(startupCtx); err != nil {
		startupCancel()
		log.Fatalf("Kafka readiness failed: %v", err)
	}
	if err := persister.Ready(startupCtx); err != nil {
		startupCancel()
		log.Fatalf("Snowflake writer readiness failed: %v", err)
	}
	startupCancel()
	if *checkDependenciesOnly {
		log.Printf("sink dependency readiness passed topics=%d writer=%s", len(topics), *writerURL)
		return
	}

	metricsServer := startMetricsServer(*metricsListen, *buildVersion, processor)
	log.Printf(
		"poker-sink build=%s group=%s topics=%s writer=%s",
		*buildVersion, *groupID, strings.Join(processor.Topics(), ","), *writerURL,
	)
	processed, runErr := kafkaClient.RunSink(ctx, processor, *maxRecords)
	if metricsServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), *requestTimeout)
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("metrics shutdown: %v", err)
		}
		cancel()
	}
	if runErr != nil {
		log.Fatalf("sink stopped: %v", runErr)
	}
	log.Printf("poker-sink stopped cleanly processed=%d metrics=%+v", processed, processor.Metrics())
}

func startMetricsServer(
	address string, buildVersion string, processor *sink.Processor,
) *http.Server {
	if strings.TrimSpace(address) == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]string{
			"status": "ok", "service": "poker-sink", "build_version": buildVersion,
		})
	})
	mux.HandleFunc("GET /metrics", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = writer.Write([]byte(processor.PrometheusMetrics()))
	})
	server := &http.Server{
		Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("sink metrics server stopped: %v", err)
		}
	}()
	return server
}

func splitNonEmpty(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func envDefault(name string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		log.Fatalf("%s must be true or false", name)
	}
	return parsed
}
