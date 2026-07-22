package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/cdc"
	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/kafkaio"
)

func main() {
	loadDotEnv("../../.env")
	brokers := flag.String("bootstrap-servers", envDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), "comma-separated Kafka brokers")
	groupID := flag.String("group-id", envDefault("KAFKA_HAND_ADAPTER_GROUP_ID", "poker-go-hand-adapter-v1"), "Kafka consumer group")
	inputTopic := flag.String("input-topic", envDefault("KAFKA_CDC_HAND_OUTBOX_TOPIC", cdc.SourceTopic), "restricted Debezium hand-outbox topic")
	outputTopic := flag.String("output-topic", envDefault("KAFKA_CDC_CANONICAL_HANDS_TOPIC", cdc.TargetTopic), "canonical completed-hand topic")
	deadLetterTopic := flag.String("dead-letter-topic", envDefault("KAFKA_DEAD_LETTER_TOPIC", "poker.pipeline.dead-letter.v1"), "sanitized CDC dead-letter topic")
	securityProtocol := flag.String("security-protocol", envDefault("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"), "PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL")
	saslMechanism := flag.String("sasl-mechanism", envDefault("KAFKA_SASL_MECHANISM", "PLAIN"), "Kafka SASL mechanism")
	datasetID := flag.String("dataset-id", envDefault("CDC_DATASET_ID", ""), "governed canonical dataset ID; required to run")
	datasetSplit := flag.String("dataset-split", envDefault("CDC_DATASET_SPLIT", "live"), "canonical dataset split")
	expectedDatabase := flag.String("expected-database", envDefault("CDC_EXPECTED_DATABASE", "poker"), "allowlisted Debezium source database")
	expectedSchema := flag.String("expected-schema", envDefault("CDC_EXPECTED_SCHEMA", "public"), "allowlisted Debezium source schema")
	expectedTable := flag.String("expected-table", envDefault("CDC_EXPECTED_TABLE", "hand_completed_outbox"), "allowlisted Debezium source table")
	allowedTenants := flag.String("allowed-tenants", envDefault("CDC_ALLOWED_TENANTS", "demo"), "comma-separated tenant allowlist; empty allows all only for development")
	allowedGameTypes := flag.String("allowed-game-types", envDefault("CDC_ALLOWED_GAME_TYPES", "NLH_CASH_6MAX,NLH_TOURNAMENT_6MAX"), "comma-separated game-type allowlist")
	buildVersion := flag.String("build-version", envDefault("CDC_SERVICE_BUILD_VERSION", "dev"), "immutable service image or source build version")
	metricsListen := flag.String("metrics-listen", envDefault("CDC_METRICS_LISTEN", "127.0.0.1:9093"), "private health/Prometheus listen address; empty disables")
	fromBeginning := flag.Bool("from-beginning", false, "start at the earliest offset when no group offset exists")
	checkKafkaOnly := flag.Bool("check-kafka-only", false, "verify Kafka authentication without consuming")
	simulationMode := flag.Bool("simulation-mode", envBool("CDC_SIMULATION_MODE", false), "run only on isolated poker.sim.* topics and sim-* datasets")
	allowSimulationCodecs := flag.Bool("allow-simulation-codecs", envBool("CDC_ALLOW_SIMULATION_CODECS", false), "enable repository-owned simulation codecs")
	maxPollRecords := flag.Int("max-poll-records", 100, "maximum records returned from one poll")
	maxRecords := flag.Int("max-records", 0, "stop after committing this many inputs; zero runs continuously")
	requestTimeout := flag.Duration("request-timeout", 10*time.Second, "Kafka startup and HTTP shutdown timeout")
	flag.Parse()
	if !*checkKafkaOnly && strings.TrimSpace(*datasetID) == "" {
		log.Fatal("CDC_DATASET_ID or --dataset-id is required")
	}
	decoders := map[string]cdc.Decoder{}
	var runtimeConfig cdc.RuntimeConfig
	if !*checkKafkaOnly {
		configured, decoderErr := configuredDecoders(*simulationMode, *allowSimulationCodecs)
		if decoderErr != nil {
			log.Fatal(decoderErr)
		}
		decoders = configured
		runtimeConfig = cdc.RuntimeConfig{
			InputTopic: *inputTopic, OutputTopic: *outputTopic,
			DeadLetterTopic: *deadLetterTopic, ServiceBuildVersion: *buildVersion,
			SimulationMode: *simulationMode,
			Adapter: cdc.Config{
				DatasetID: *datasetID, DatasetSplit: *datasetSplit,
				ExpectedDB: *expectedDatabase, ExpectedSchema: *expectedSchema,
				ExpectedTable:    *expectedTable,
				AllowedTenants:   valueAllowlist(*allowedTenants),
				AllowedGameTypes: valueAllowlist(*allowedGameTypes),
			},
		}
		var configErr error
		runtimeConfig, configErr = cdc.ValidateRuntimeConfig(runtimeConfig)
		if configErr != nil {
			log.Fatalf("configure CDC runtime: %v", configErr)
		}
	}

	kafkaClient, err := kafkaio.NewClient(kafkaio.Config{
		Brokers: strings.Split(*brokers, ","), ClientID: "poker-go-hand-adapter-v1",
		GroupID: *groupID, InputTopic: *inputTopic,
		SecurityProtocol: *securityProtocol, SASLMechanism: *saslMechanism,
		Username: os.Getenv("KAFKA_SASL_USERNAME"), Password: os.Getenv("KAFKA_SASL_PASSWORD"),
		FromBeginning: *fromBeginning, MaxPollRecords: *maxPollRecords,
	})
	if err != nil {
		log.Fatalf("configure Kafka: %v", err)
	}
	defer kafkaClient.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	startupCtx, startupCancel := context.WithTimeout(ctx, *requestTimeout)
	if err := kafkaClient.Ping(startupCtx); err != nil {
		startupCancel()
		log.Fatalf("Kafka readiness failed: %v", err)
	}
	startupCancel()
	if *checkKafkaOnly {
		log.Printf("hand-adapter Kafka readiness passed input=%s output=%s", *inputTopic, *outputTopic)
		return
	}
	processor, err := cdc.NewRuntimeProcessor(
		runtimeConfig, decoders, kafkaClient, kafkaClient,
	)
	if err != nil {
		log.Fatalf("configure CDC processor: %v", err)
	}
	metricsServer := startMetricsServer(*metricsListen, *buildVersion, processor)
	log.Printf(
		"hand-adapter build=%s input=%s output=%s dlq=%s dataset=%s split=%s simulation=%t simulation_codecs=%t",
		*buildVersion, *inputTopic, *outputTopic, *deadLetterTopic,
		*datasetID, *datasetSplit, *simulationMode, *allowSimulationCodecs,
	)
	processed, runErr := kafkaClient.RunAdapter(ctx, processor, *maxRecords)
	if metricsServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), *requestTimeout)
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("metrics shutdown: %v", err)
		}
		cancel()
	}
	if runErr != nil {
		log.Fatalf("adapter stopped: %v", runErr)
	}
	log.Printf("hand-adapter stopped cleanly processed=%d metrics=%+v", processed, processor.Metrics())
}

func configuredDecoders(simulationMode, allowSimulationCodecs bool) (map[string]cdc.Decoder, error) {
	if allowSimulationCodecs && !simulationMode {
		return nil, fmt.Errorf("--allow-simulation-codecs requires --simulation-mode")
	}
	if simulationMode && !allowSimulationCodecs {
		return nil, fmt.Errorf("--simulation-mode requires --allow-simulation-codecs")
	}
	if simulationMode {
		return map[string]cdc.Decoder{
			cdc.FixtureCodec:            cdc.CanonicalJSONDecoder{},
			cdc.SimulationProtobufCodec: cdc.SimulationProtobufDecoder{},
		}, nil
	}
	return nil, fmt.Errorf(
		"no production poker-server decoder is registered; this project currently supports isolated simulation only",
	)
}

func valueAllowlist(value string) map[string]bool {
	allowlist := map[string]bool{}
	for _, tenant := range splitNonEmpty(value) {
		allowlist[tenant] = true
	}
	return allowlist
}

func startMetricsServer(address, buildVersion string, processor *cdc.Processor) *http.Server {
	if strings.TrimSpace(address) == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]string{
			"status": "ok", "service": "poker-hand-adapter", "build_version": buildVersion,
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
			log.Printf("metrics server stopped: %v", err)
		}
	}()
	log.Printf("hand-adapter metrics listen=%s", address)
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

func envDefault(name, fallback string) string {
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

func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("read %s: %v", path, err)
		}
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, found := strings.Cut(strings.TrimPrefix(line, "export "), "=")
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		if !found || name == "" || os.Getenv(name) != "" {
			continue
		}
		if unquoted, err := strconv.Unquote(value); err == nil {
			value = unquoted
		}
		if err := os.Setenv(name, value); err != nil {
			log.Printf("set %s from %s: %v", name, path, err)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
	}
}
