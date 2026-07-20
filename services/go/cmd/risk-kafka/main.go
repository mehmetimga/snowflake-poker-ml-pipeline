package main

import (
	"bufio"
	"context"
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

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/kafkaio"
	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/risk"
	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/stream"
)

func main() {
	loadDotEnv("../../.env")
	modelDir := flag.String("model-dir", "../../models/pair-catboost-full-v2", "model artifact directory")
	tritonURL := flag.String("triton-url", envDefault("TRITON_HTTP_URL", "http://127.0.0.1:8000"), "Triton V2 HTTP base URL")
	brokers := flag.String("bootstrap-servers", envDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), "comma-separated Kafka brokers")
	groupID := flag.String("group-id", envDefault("KAFKA_RISK_SCORER_GROUP_ID", "poker-go-risk-scorer-v1"), "Kafka consumer group")
	inputTopic := flag.String("input-topic", envDefault("KAFKA_PAIR_FEATURES_TOPIC", "poker.pair-features.v1"), "pair-feature input topic")
	scoresTopic := flag.String("scores-topic", envDefault("KAFKA_RISK_SCORES_TOPIC", "poker.risk-scores.v1"), "risk-score output topic")
	alertsTopic := flag.String("alerts-topic", envDefault("KAFKA_RISK_ALERTS_TOPIC", "poker.risk-alerts.v1"), "risk-alert output topic")
	deadLetterTopic := flag.String("dead-letter-topic", envDefault("KAFKA_DEAD_LETTER_TOPIC", "poker.pipeline.dead-letter.v1"), "dead-letter topic")
	securityProtocol := flag.String("security-protocol", envDefault("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"), "PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL")
	saslMechanism := flag.String("sasl-mechanism", envDefault("KAFKA_SASL_MECHANISM", "PLAIN"), "Kafka SASL mechanism")
	fromBeginning := flag.Bool("from-beginning", false, "start at the earliest offset when no group offset exists")
	checkKafkaOnly := flag.Bool("check-kafka-only", false, "verify Kafka authentication without starting the scoring loop")
	maxPollRecords := flag.Int("max-poll-records", 500, "maximum records processed from one poll")
	maxScores := flag.Int("max-scores", 0, "stop after publishing this many complete-hand scores; zero runs continuously")
	assemblyTTL := flag.Duration("assembly-ttl", 30*time.Minute, "complete-hand correction cache TTL")
	requestTimeout := flag.Duration("request-timeout", 10*time.Second, "Triton and startup request timeout")
	allowedTenants := flag.String("allowed-tenants", envDefault("RISK_ALLOWED_TENANTS", ""), "comma-separated tenant allowlist; empty allows all for development")
	buildVersion := flag.String("build-version", envDefault("RISK_SERVICE_BUILD_VERSION", "dev"), "immutable service image or source build version")
	flag.Parse()

	bundle, err := risk.LoadArtifactBundle(*modelDir)
	if err != nil {
		log.Fatalf("load model artifacts: %v", err)
	}
	httpClient := &http.Client{Timeout: *requestTimeout}
	backend, err := risk.NewTritonBackend(*tritonURL, bundle.Contract.Batching.TritonModel, httpClient)
	if err != nil {
		log.Fatalf("configure Triton backend: %v", err)
	}
	scorer, err := risk.NewScorerWithBuildVersion(bundle, backend, nil, *buildVersion)
	if err != nil {
		log.Fatalf("configure scorer: %v", err)
	}
	assembler, err := risk.NewHandAssembler(bundle.Contract.Batching.ExpectedPairsPerSixPlayerHand, *assemblyTTL)
	if err != nil {
		log.Fatalf("configure hand assembler: %v", err)
	}
	kafkaClient, err := kafkaio.NewClient(kafkaio.Config{
		Brokers: strings.Split(*brokers, ","), ClientID: "poker-go-risk-scorer-v1",
		GroupID: *groupID, InputTopic: *inputTopic,
		SecurityProtocol: *securityProtocol, SASLMechanism: *saslMechanism,
		Username: os.Getenv("KAFKA_SASL_USERNAME"), Password: os.Getenv("KAFKA_SASL_PASSWORD"),
		FromBeginning: *fromBeginning, MaxPollRecords: *maxPollRecords,
	})
	if err != nil {
		log.Fatalf("configure Kafka: %v", err)
	}
	defer kafkaClient.Close()
	processor, err := stream.NewProcessor(stream.Config{
		InputTopic: *inputTopic, RiskScoresTopic: *scoresTopic,
		RiskAlertsTopic: *alertsTopic, DeadLetterTopic: *deadLetterTopic,
		AllowedTenants: splitNonEmpty(*allowedTenants),
	}, scorer, assembler, kafkaClient, kafkaClient, nil)
	if err != nil {
		log.Fatalf("configure stream processor: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	startupCtx, cancel := context.WithTimeout(ctx, *requestTimeout)
	if err := kafkaClient.Ping(startupCtx); err != nil {
		cancel()
		log.Fatalf("Kafka readiness failed: %v", err)
	}
	if *checkKafkaOnly {
		cancel()
		log.Printf("Kafka readiness passed brokers=%s input=%s", *brokers, *inputTopic)
		return
	}
	if err := scorer.Ready(startupCtx); err != nil {
		cancel()
		log.Fatalf("Triton readiness failed: %v", err)
	}
	cancel()
	log.Printf("risk-kafka model=%s run=%s input=%s scores=%s alerts=%s", bundle.Contract.ModelName, bundle.Contract.RunID, *inputTopic, *scoresTopic, *alertsTopic)
	scores, err := kafkaClient.Run(ctx, processor, *maxScores)
	if err != nil {
		log.Fatalf("stream stopped: %v", err)
	}
	log.Printf("risk-kafka stopped cleanly scores=%d", scores)
}

func splitNonEmpty(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	return parts
}

func envDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
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
