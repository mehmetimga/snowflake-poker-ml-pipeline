package cdc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/stream"
)

const DeadLetterEventType = "poker.cdc-hand.dead-lettered"

type RuntimeConfig struct {
	InputTopic          string
	OutputTopic         string
	DeadLetterTopic     string
	ServiceBuildVersion string
	SimulationMode      bool
	Adapter             Config
}

type RuntimeMetrics struct {
	InputRecords       int64
	CanonicalPublished int64
	DeadLetters        int64
	CommittedRecords   int64
	PublishFailures    int64
	CommitFailures     int64
}

type Processor struct {
	config    RuntimeConfig
	decoders  map[string]Decoder
	publisher stream.Publisher
	committer stream.Committer
	offsets   *stream.OffsetTracker
	metricsMu sync.Mutex
	metrics   RuntimeMetrics
}

type deadLetterPayload struct {
	SourceTopic         string `json:"source_topic"`
	SourcePartition     int32  `json:"source_partition"`
	SourceOffset        int64  `json:"source_offset"`
	SourceTimestamp     string `json:"source_timestamp"`
	SourceKeySHA256     string `json:"source_key_sha256"`
	SourceValueSHA256   string `json:"source_value_sha256"`
	ErrorCode           string `json:"error_code"`
	ServiceBuildVersion string `json:"service_build_version"`
}

type deadLetterEvent struct {
	EventID       string            `json:"event_id"`
	EventType     string            `json:"event_type"`
	SchemaVersion int               `json:"schema_version"`
	OccurredAt    string            `json:"occurred_at"`
	EmittedAt     string            `json:"emitted_at"`
	Payload       deadLetterPayload `json:"payload"`
}

func NewRuntimeProcessor(
	config RuntimeConfig,
	decoders map[string]Decoder,
	publisher stream.Publisher,
	committer stream.Committer,
) (*Processor, error) {
	var err error
	config, err = ValidateRuntimeConfig(config)
	if err != nil {
		return nil, err
	}
	if publisher == nil || committer == nil {
		return nil, fmt.Errorf("publisher and committer are required")
	}
	if decoders == nil {
		decoders = map[string]Decoder{}
	}
	return &Processor{
		config: config, decoders: decoders, publisher: publisher,
		committer: committer, offsets: stream.NewOffsetTracker(),
	}, nil
}

// ValidateRuntimeConfig applies defaults and prevents simulation traffic from
// sharing production CDC, canonical, or dead-letter topics.
func ValidateRuntimeConfig(config RuntimeConfig) (RuntimeConfig, error) {
	if config.InputTopic == "" || config.OutputTopic == "" || config.DeadLetterTopic == "" {
		return RuntimeConfig{}, fmt.Errorf("input, canonical output, and dead-letter topics are required")
	}
	expectedInput, expectedOutput, expectedDeadLetter := SourceTopic, TargetTopic, DeadLetterTopic
	if config.SimulationMode {
		expectedInput = SimulationSourceTopic
		expectedOutput = SimulationTargetTopic
		expectedDeadLetter = SimulationDeadLetterTopic
	}
	if config.InputTopic != expectedInput {
		return RuntimeConfig{}, fmt.Errorf("CDC input topic must be %s", expectedInput)
	}
	if config.OutputTopic != expectedOutput {
		return RuntimeConfig{}, fmt.Errorf("canonical output topic must be %s", expectedOutput)
	}
	if config.DeadLetterTopic != expectedDeadLetter {
		return RuntimeConfig{}, fmt.Errorf("dead-letter topic must be %s", expectedDeadLetter)
	}
	if config.ServiceBuildVersion == "" {
		return RuntimeConfig{}, fmt.Errorf("service build version is required")
	}
	config.Adapter = config.Adapter.withDefaults()
	if config.Adapter.DatasetID == "" {
		return RuntimeConfig{}, fmt.Errorf("adapter dataset ID is required")
	}
	if config.SimulationMode && !strings.HasPrefix(config.Adapter.DatasetID, "sim-") {
		return RuntimeConfig{}, fmt.Errorf("simulation dataset ID must start with sim-")
	}
	return config, nil
}

func (processor *Processor) Handle(
	ctx context.Context,
	record stream.InputRecord,
) (stream.ProcessResult, error) {
	processor.offsets.Observe(record.RecordRef)
	processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.InputRecords++ })
	if record.Topic != processor.config.InputTopic {
		return processor.deadLetter(ctx, record, "unexpected_topic")
	}
	adapted, err := Adapt(
		record.Value,
		processor.config.Adapter,
		processor.decoders,
		&SourcePosition{
			Topic: record.Topic, Partition: int(record.Partition), Offset: record.Offset,
		},
	)
	if err != nil {
		var rejected *RejectError
		code := "decoder_failure"
		if errors.As(err, &rejected) {
			code = rejected.Code
		}
		return processor.deadLetter(ctx, record, code)
	}
	value, err := json.Marshal(adapted.Event)
	if err != nil {
		return stream.ProcessResult{}, fmt.Errorf("marshal canonical hand: %w", err)
	}
	headers := make([]stream.Header, 0, len(adapted.Headers))
	for _, header := range adapted.Headers {
		headers = append(headers, stream.Header{
			Key: header.Key, Value: []byte(header.Value),
		})
	}
	output := stream.OutputRecord{
		Topic: processor.config.OutputTopic, Key: []byte(adapted.PartitionKey),
		Value: value, Headers: headers,
	}
	if err := processor.publisher.Publish(ctx, []stream.OutputRecord{output}); err != nil {
		processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.PublishFailures++ })
		return stream.ProcessResult{}, fmt.Errorf("publish canonical hand: %w", err)
	}
	processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.CanonicalPublished++ })
	committed, err := processor.markAndCommit(ctx, record.RecordRef)
	if err != nil {
		return stream.ProcessResult{}, err
	}
	return stream.ProcessResult{
		Status: "published", HandID: adapted.HandID,
		OutputCount: 1, Committed: committed,
	}, nil
}

func (processor *Processor) deadLetter(
	ctx context.Context,
	record stream.InputRecord,
	code string,
) (stream.ProcessResult, error) {
	timestamp := record.Timestamp.UTC()
	if record.Timestamp.IsZero() {
		timestamp = time.Unix(0, 0).UTC()
	}
	timeText := timestamp.Format(time.RFC3339Nano)
	keyDigest := sha256.Sum256(record.Key)
	valueDigest := sha256.Sum256(record.Value)
	identity := strings.Join([]string{
		"cdc-hand-dlq", processor.config.ServiceBuildVersion, record.Topic,
		strconv.FormatInt(int64(record.Partition), 10),
		strconv.FormatInt(record.Offset, 10), code,
	}, ":")
	eventID := uuidV5URL(identity)
	event := deadLetterEvent{
		EventID: eventID, EventType: DeadLetterEventType, SchemaVersion: 1,
		OccurredAt: timeText, EmittedAt: timeText,
		Payload: deadLetterPayload{
			SourceTopic: record.Topic, SourcePartition: record.Partition,
			SourceOffset: record.Offset, SourceTimestamp: timeText,
			SourceKeySHA256:   hex.EncodeToString(keyDigest[:]),
			SourceValueSHA256: hex.EncodeToString(valueDigest[:]),
			ErrorCode:         code, ServiceBuildVersion: processor.config.ServiceBuildVersion,
		},
	}
	value, err := json.Marshal(event)
	if err != nil {
		return stream.ProcessResult{}, fmt.Errorf("marshal CDC dead letter: %w", err)
	}
	headers := []stream.Header{
		{Key: "event_id", Value: []byte(eventID)},
		{Key: "event_type", Value: []byte(DeadLetterEventType)},
		{Key: "error_code", Value: []byte(code)},
		{Key: "cdc_source_topic", Value: []byte(record.Topic)},
		{Key: "cdc_source_partition", Value: []byte(strconv.FormatInt(int64(record.Partition), 10))},
		{Key: "cdc_source_offset", Value: []byte(strconv.FormatInt(record.Offset, 10))},
	}
	if err := processor.publisher.Publish(ctx, []stream.OutputRecord{{
		Topic: processor.config.DeadLetterTopic, Key: []byte(eventID),
		Value: value, Headers: headers,
	}}); err != nil {
		processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.PublishFailures++ })
		return stream.ProcessResult{}, fmt.Errorf("publish CDC dead letter: %w", err)
	}
	processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.DeadLetters++ })
	committed, err := processor.markAndCommit(ctx, record.RecordRef)
	if err != nil {
		return stream.ProcessResult{}, err
	}
	return stream.ProcessResult{
		Status: "dead_lettered", OutputCount: 1, Committed: committed,
	}, nil
}

func (processor *Processor) markAndCommit(
	ctx context.Context,
	record stream.RecordRef,
) ([]stream.RecordRef, error) {
	processor.offsets.MarkProcessed(record)
	ready := processor.offsets.Ready()
	if len(ready) == 0 {
		return nil, nil
	}
	if err := processor.committer.Commit(ctx, ready); err != nil {
		processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.CommitFailures++ })
		return nil, fmt.Errorf("commit acknowledged CDC offsets: %w", err)
	}
	processor.offsets.Acknowledge(ready)
	processor.updateMetrics(func(metrics *RuntimeMetrics) {
		metrics.CommittedRecords++
	})
	return ready, nil
}

func (processor *Processor) updateMetrics(update func(*RuntimeMetrics)) {
	processor.metricsMu.Lock()
	defer processor.metricsMu.Unlock()
	update(&processor.metrics)
}

func (processor *Processor) Metrics() RuntimeMetrics {
	processor.metricsMu.Lock()
	defer processor.metricsMu.Unlock()
	return processor.metrics
}

func (processor *Processor) PrometheusMetrics() string {
	metrics := processor.Metrics()
	return fmt.Sprintf(
		"# HELP poker_cdc_adapter_input_records_total CDC input records observed.\n"+
			"# TYPE poker_cdc_adapter_input_records_total counter\n"+
			"poker_cdc_adapter_input_records_total %d\n"+
			"# HELP poker_cdc_adapter_canonical_published_total Canonical hand writes acknowledged by Kafka.\n"+
			"# TYPE poker_cdc_adapter_canonical_published_total counter\n"+
			"poker_cdc_adapter_canonical_published_total %d\n"+
			"# HELP poker_cdc_adapter_dead_letters_total Sanitized CDC dead-letter writes acknowledged by Kafka.\n"+
			"# TYPE poker_cdc_adapter_dead_letters_total counter\n"+
			"poker_cdc_adapter_dead_letters_total %d\n"+
			"# HELP poker_cdc_adapter_committed_records_total Source records committed after acknowledged output.\n"+
			"# TYPE poker_cdc_adapter_committed_records_total counter\n"+
			"poker_cdc_adapter_committed_records_total %d\n"+
			"# HELP poker_cdc_adapter_publish_failures_total Kafka output acknowledgement failures.\n"+
			"# TYPE poker_cdc_adapter_publish_failures_total counter\n"+
			"poker_cdc_adapter_publish_failures_total %d\n"+
			"# HELP poker_cdc_adapter_commit_failures_total Source offset commit failures after output acknowledgement.\n"+
			"# TYPE poker_cdc_adapter_commit_failures_total counter\n"+
			"poker_cdc_adapter_commit_failures_total %d\n",
		metrics.InputRecords, metrics.CanonicalPublished, metrics.DeadLetters,
		metrics.CommittedRecords, metrics.PublishFailures, metrics.CommitFailures,
	)
}
