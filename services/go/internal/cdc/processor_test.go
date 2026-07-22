package cdc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/stream"
)

type runtimePublisher struct {
	records []stream.OutputRecord
	steps   *[]string
	err     error
}

func (publisher *runtimePublisher) Publish(_ context.Context, records []stream.OutputRecord) error {
	for _, record := range records {
		*publisher.steps = append(*publisher.steps, "publish:"+record.Topic)
	}
	if publisher.err != nil {
		return publisher.err
	}
	for _, record := range records {
		copy := record
		copy.Key = append([]byte(nil), record.Key...)
		copy.Value = append([]byte(nil), record.Value...)
		copy.Headers = append([]stream.Header(nil), record.Headers...)
		publisher.records = append(publisher.records, copy)
	}
	return nil
}

type runtimeCommitter struct {
	calls [][]stream.RecordRef
	steps *[]string
	err   error
}

func (committer *runtimeCommitter) Commit(_ context.Context, records []stream.RecordRef) error {
	*committer.steps = append(*committer.steps, "commit")
	if committer.err != nil {
		return committer.err
	}
	committer.calls = append(committer.calls, append([]stream.RecordRef(nil), records...))
	return nil
}

func runtimeConfig() RuntimeConfig {
	return RuntimeConfig{
		InputTopic: SourceTopic, OutputTopic: TargetTopic,
		DeadLetterTopic:     DeadLetterTopic,
		ServiceBuildVersion: "adapter-test-build",
		Adapter:             testConfig(),
	}
}

func TestRuntimeSimulationTopicsAreStrictlyIsolated(t *testing.T) {
	simulation := runtimeConfig()
	simulation.SimulationMode = true
	simulation.InputTopic = SimulationSourceTopic
	simulation.OutputTopic = SimulationTargetTopic
	simulation.DeadLetterTopic = SimulationDeadLetterTopic
	simulation.Adapter.DatasetID = "sim-cdc-v1"
	if _, err := ValidateRuntimeConfig(simulation); err != nil {
		t.Fatalf("valid simulation configuration rejected: %v", err)
	}
	steps := []string{}
	publisher := &runtimePublisher{steps: &steps}
	committer := &runtimeCommitter{steps: &steps}
	processor, err := NewRuntimeProcessor(
		simulation,
		map[string]Decoder{FixtureCodec: CanonicalJSONDecoder{}},
		publisher,
		committer,
	)
	if err != nil {
		t.Fatal(err)
	}
	record := runtimeRecord(t)
	record.Topic = SimulationSourceTopic
	if _, err := processor.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(publisher.records) != 1 || publisher.records[0].Topic != SimulationTargetTopic {
		t.Fatalf("simulation escaped its canonical topic: %+v", publisher.records)
	}
	if !reflect.DeepEqual(steps, []string{"publish:" + SimulationTargetTopic, "commit"}) {
		t.Fatalf("simulation acknowledgement order changed: %v", steps)
	}

	for _, mutate := range []func(*RuntimeConfig){
		func(config *RuntimeConfig) { config.InputTopic = SourceTopic },
		func(config *RuntimeConfig) { config.OutputTopic = TargetTopic },
		func(config *RuntimeConfig) { config.DeadLetterTopic = DeadLetterTopic },
		func(config *RuntimeConfig) { config.Adapter.DatasetID = "poker-live-v1" },
	} {
		invalid := simulation
		mutate(&invalid)
		if _, err := ValidateRuntimeConfig(invalid); err == nil {
			t.Fatalf("unsafe simulation configuration was accepted: %+v", invalid)
		}
	}

	production := runtimeConfig()
	production.InputTopic = SimulationSourceTopic
	if _, err := ValidateRuntimeConfig(production); err == nil {
		t.Fatal("production mode accepted a simulation source topic")
	}
}

func runtimeRecord(t *testing.T) stream.InputRecord {
	t.Helper()
	return stream.InputRecord{
		RecordRef: stream.RecordRef{Topic: SourceTopic, Partition: 2, Offset: 41},
		Key:       []byte("22222222-2222-4222-8222-222222222222"),
		Value:     fixtureBytes(t, "debezium.hand-completed-outbox.v1.json"),
		Timestamp: time.Date(2026, 7, 21, 10, 0, 1, 50_000_000, time.UTC),
	}
}

func newRuntimeProcessor(
	t *testing.T,
	decoders map[string]Decoder,
	publisher *runtimePublisher,
	committer *runtimeCommitter,
) *Processor {
	t.Helper()
	processor, err := NewRuntimeProcessor(
		runtimeConfig(), decoders, publisher, committer,
	)
	if err != nil {
		t.Fatal(err)
	}
	return processor
}

func outputHeaders(record stream.OutputRecord) map[string]string {
	values := make(map[string]string, len(record.Headers))
	for _, header := range record.Headers {
		values[header.Key] = string(header.Value)
	}
	return values
}

func TestRuntimePublishesCanonicalHandBeforeCommittingSource(t *testing.T) {
	steps := []string{}
	publisher := &runtimePublisher{steps: &steps}
	committer := &runtimeCommitter{steps: &steps}
	processor := newRuntimeProcessor(
		t, map[string]Decoder{FixtureCodec: CanonicalJSONDecoder{}},
		publisher, committer,
	)

	result, err := processor.Handle(context.Background(), runtimeRecord(t))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(steps, []string{"publish:" + TargetTopic, "commit"}) {
		t.Fatalf("output must be acknowledged before commit: %v", steps)
	}
	if result.Status != "published" || result.HandID != "C2-FIXTURE-H-000001" || result.OutputCount != 1 {
		t.Fatalf("unexpected runtime result: %+v", result)
	}
	if len(publisher.records) != 1 || string(publisher.records[0].Key) != "c2_table_01" {
		t.Fatalf("unexpected canonical output: %+v", publisher.records)
	}
	var event Event
	if err := json.Unmarshal(publisher.records[0].Value, &event); err != nil {
		t.Fatal(err)
	}
	if event.EventID != "f00d27af-a72b-58bd-8180-14d6e38d3040" {
		t.Fatalf("canonical identity changed: %+v", event)
	}
	headers := outputHeaders(publisher.records[0])
	if headers["cdc_source_lsn"] != "270113177" || headers["cdc_source_offset"] != "41" {
		t.Fatalf("canonical source lineage is incomplete: %v", headers)
	}
	metrics := processor.Metrics()
	if metrics.InputRecords != 1 || metrics.CanonicalPublished != 1 || metrics.CommittedRecords != 1 || metrics.DeadLetters != 0 {
		t.Fatalf("unexpected acknowledged metrics: %+v", metrics)
	}
}

func TestRuntimeFailsClosedWithoutExplicitCodecRegistration(t *testing.T) {
	steps := []string{}
	publisher := &runtimePublisher{steps: &steps}
	committer := &runtimeCommitter{steps: &steps}
	processor := newRuntimeProcessor(t, nil, publisher, committer)

	result, err := processor.Handle(context.Background(), runtimeRecord(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "dead_lettered" || len(publisher.records) != 1 {
		t.Fatalf("unregistered fixture codec did not fail closed: %+v", result)
	}
	headers := outputHeaders(publisher.records[0])
	if headers["error_code"] != "unknown_codec_version" {
		t.Fatalf("wrong fail-closed reason: %v", headers)
	}
}

func TestRuntimeDeadLetterIsSanitizedDeterministicAndCommittedAfterPublish(t *testing.T) {
	record := runtimeRecord(t)
	record.Value = []byte(`{"secret_binary":"must-not-escape"}`)
	run := func() (stream.OutputRecord, []string) {
		steps := []string{}
		publisher := &runtimePublisher{steps: &steps}
		committer := &runtimeCommitter{steps: &steps}
		processor := newRuntimeProcessor(t, map[string]Decoder{}, publisher, committer)
		result, err := processor.Handle(context.Background(), record)
		if err != nil || result.Status != "dead_lettered" {
			t.Fatalf("poison record was not durably isolated: result=%+v err=%v", result, err)
		}
		return publisher.records[0], steps
	}

	first, firstSteps := run()
	second, _ := run()
	if !reflect.DeepEqual(firstSteps, []string{"publish:poker.pipeline.dead-letter.v1", "commit"}) {
		t.Fatalf("DLQ must be acknowledged before commit: %v", firstSteps)
	}
	if !bytes.Equal(first.Value, second.Value) || !reflect.DeepEqual(first.Headers, second.Headers) {
		t.Fatal("same poison source position did not produce deterministic DLQ output")
	}
	if bytes.Contains(first.Value, []byte("must-not-escape")) || bytes.Contains(first.Value, record.Value) {
		t.Fatal("raw proprietary CDC value leaked into DLQ")
	}
	var event deadLetterEvent
	if err := json.Unmarshal(first.Value, &event); err != nil {
		t.Fatal(err)
	}
	if event.EventType != DeadLetterEventType || event.Payload.ErrorCode != "invalid_envelope" || len(event.Payload.SourceValueSHA256) != 64 {
		t.Fatalf("invalid sanitized DLQ contract: %+v", event)
	}
}

func TestRuntimeNeverCommitsWhenCanonicalOrDLQPublishFails(t *testing.T) {
	for _, test := range []struct {
		name     string
		record   func(*testing.T) stream.InputRecord
		decoders map[string]Decoder
	}{
		{
			name: "canonical", record: runtimeRecord,
			decoders: map[string]Decoder{FixtureCodec: CanonicalJSONDecoder{}},
		},
		{
			name: "dead-letter", record: func(t *testing.T) stream.InputRecord {
				value := runtimeRecord(t)
				value.Value = []byte("{")
				return value
			}, decoders: map[string]Decoder{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			steps := []string{}
			publisher := &runtimePublisher{steps: &steps, err: errors.New("broker unavailable")}
			committer := &runtimeCommitter{steps: &steps}
			processor := newRuntimeProcessor(t, test.decoders, publisher, committer)
			if _, err := processor.Handle(context.Background(), test.record(t)); err == nil {
				t.Fatal("expected publish failure")
			}
			if len(committer.calls) != 0 || slicesContain(steps, "commit") {
				t.Fatalf("failed output must remain uncommitted: %v", steps)
			}
			if processor.Metrics().PublishFailures != 1 || processor.Metrics().CommittedRecords != 0 {
				t.Fatalf("publish failure metrics are wrong: %+v", processor.Metrics())
			}
		})
	}
}

func TestRuntimeCommitFailureReplaysByteIdenticalCanonicalOutput(t *testing.T) {
	record := runtimeRecord(t)
	steps := []string{}
	firstPublisher := &runtimePublisher{steps: &steps}
	firstCommitter := &runtimeCommitter{steps: &steps, err: errors.New("coordinator unavailable")}
	first := newRuntimeProcessor(
		t, map[string]Decoder{FixtureCodec: CanonicalJSONDecoder{}},
		firstPublisher, firstCommitter,
	)
	if _, err := first.Handle(context.Background(), record); err == nil {
		t.Fatal("expected commit failure")
	}
	if first.Metrics().CommitFailures != 1 || first.Metrics().CommittedRecords != 0 {
		t.Fatalf("commit failure metrics are wrong: %+v", first.Metrics())
	}

	steps = []string{}
	secondPublisher := &runtimePublisher{steps: &steps}
	secondCommitter := &runtimeCommitter{steps: &steps}
	second := newRuntimeProcessor(
		t, map[string]Decoder{FixtureCodec: CanonicalJSONDecoder{}},
		secondPublisher, secondCommitter,
	)
	if _, err := second.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(firstPublisher.records[0], secondPublisher.records[0]) {
		t.Fatal("replay after commit failure changed canonical output")
	}
}

func TestRuntimeMutableCDCRecordGoesToDLQAndMetricsAreExposed(t *testing.T) {
	recordValue := fixtureRecord(t)
	recordValue["op"] = "u"
	record := runtimeRecord(t)
	record.Value = encodeRecord(t, recordValue)
	steps := []string{}
	publisher := &runtimePublisher{steps: &steps}
	committer := &runtimeCommitter{steps: &steps}
	processor := newRuntimeProcessor(
		t, map[string]Decoder{FixtureCodec: CanonicalJSONDecoder{}},
		publisher, committer,
	)

	result, err := processor.Handle(context.Background(), record)
	if err != nil || result.Status != "dead_lettered" {
		t.Fatalf("mutable CDC record escaped DLQ: result=%+v err=%v", result, err)
	}
	if outputHeaders(publisher.records[0])["error_code"] != "immutable_outbox_operation" {
		t.Fatal("mutable-record DLQ reason is missing")
	}
	metrics := processor.PrometheusMetrics()
	for _, expected := range []string{
		"poker_cdc_adapter_input_records_total 1",
		"poker_cdc_adapter_dead_letters_total 1",
		"poker_cdc_adapter_committed_records_total 1",
	} {
		if !strings.Contains(metrics, expected) {
			t.Fatalf("metrics missing %q:\n%s", expected, metrics)
		}
	}
}

func slicesContain(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
