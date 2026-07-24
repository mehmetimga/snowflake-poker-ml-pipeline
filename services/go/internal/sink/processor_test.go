package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/stream"
)

type fakePersister struct {
	requests []PersistRequest
	result   PersistResult
	err      error
	order    *[]string
}

func (persister *fakePersister) Persist(
	_ context.Context, request PersistRequest,
) (PersistResult, error) {
	persister.requests = append(persister.requests, request)
	if persister.order != nil {
		*persister.order = append(*persister.order, "persist")
	}
	return persister.result, persister.err
}

type fakeCommitter struct {
	calls [][]stream.RecordRef
	err   error
	order *[]string
}

func (committer *fakeCommitter) Commit(
	_ context.Context, records []stream.RecordRef,
) error {
	if committer.order != nil {
		*committer.order = append(*committer.order, "commit")
	}
	if committer.err != nil {
		return committer.err
	}
	committer.calls = append(committer.calls, append([]stream.RecordRef(nil), records...))
	return nil
}

func validRiskRecord() stream.InputRecord {
	return stream.InputRecord{
		RecordRef: stream.RecordRef{
			Topic: "poker.synthetic.risk-scores.v1", Partition: 2, Offset: 41,
		},
		Key: []byte("hand-1"),
		Value: []byte(`{
			"event_id":"00000000-0000-5000-8000-000000000041",
			"event_type":"poker.risk-score.computed",
			"schema_version":1,
			"tenant_id":"demo",
			"product_id":"poker",
			"dataset_id":"acceptance-d7",
			"dataset_split":"live",
			"occurred_at":"2026-07-23T10:00:00Z",
			"emitted_at":"2026-07-23T10:00:01Z",
			"trace_id":"00000000-0000-5000-8000-000000000099",
			"payload":{"score_id":"score-1","hand_id":"hand-1","table_id":"table-1"}
		}`),
		Timestamp: time.Date(2026, 7, 23, 10, 0, 1, 0, time.UTC),
	}
}

func newTestProcessor(
	t *testing.T, persister *fakePersister, committer *fakeCommitter,
) *Processor {
	t.Helper()
	processor, err := NewProcessor(Config{
		Routes: CanonicalSyntheticRoutes(), AllowedTenants: []string{"demo"},
		ServiceBuildVersion: "sink-test-build",
	}, persister, committer)
	if err != nil {
		t.Fatal(err)
	}
	return processor
}

func TestProcessorPersistsBeforeCommit(t *testing.T) {
	order := []string{}
	persister := &fakePersister{
		result: PersistResult{Status: "inserted"}, order: &order,
	}
	committer := &fakeCommitter{order: &order}
	processor := newTestProcessor(t, persister, committer)

	result, err := processor.Handle(context.Background(), validRiskRecord())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "persist,commit" {
		t.Fatalf("unexpected operation order: %v", order)
	}
	if result.Status != "inserted" || len(result.Committed) != 1 ||
		result.Committed[0].Offset != 41 {
		t.Fatalf("unexpected result: %+v", result)
	}
	request := persister.requests[0]
	if request.Kind != KindRiskScore || request.EventID == "" ||
		len(request.Event) == 0 || request.Kafka.Topic != validRiskRecord().Topic {
		t.Fatalf("unexpected persistence request: %+v", request)
	}
}

func TestDuplicateStillCommits(t *testing.T) {
	persister := &fakePersister{result: PersistResult{Status: "duplicate"}}
	committer := &fakeCommitter{}
	processor := newTestProcessor(t, persister, committer)

	result, err := processor.Handle(context.Background(), validRiskRecord())
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "duplicate" || len(committer.calls) != 1 {
		t.Fatalf("duplicate did not advance its offset: result=%+v commits=%v", result, committer.calls)
	}
}

func TestEventHashIgnoresInsignificantJSONWhitespace(t *testing.T) {
	persister := &fakePersister{result: PersistResult{Status: "inserted"}}
	committer := &fakeCommitter{}
	processor := newTestProcessor(t, persister, committer)
	formatted := validRiskRecord()
	compact := formatted
	compact.Offset++
	var compactValue bytes.Buffer
	if err := json.Compact(&compactValue, formatted.Value); err != nil {
		t.Fatal(err)
	}
	compact.Value = compactValue.Bytes()

	if _, err := processor.Handle(context.Background(), formatted); err != nil {
		t.Fatal(err)
	}
	if _, err := processor.Handle(context.Background(), compact); err != nil {
		t.Fatal(err)
	}
	first, second := persister.requests[0], persister.requests[1]
	if first.EventSHA256 != second.EventSHA256 {
		t.Fatalf(
			"whitespace changed immutable event hash: %s != %s",
			first.EventSHA256, second.EventSHA256,
		)
	}
	if first.Kafka.ValueSHA256 == second.Kafka.ValueSHA256 {
		t.Fatal("raw Kafka hashes must preserve byte-level serialization changes")
	}
	if second.EventSHA256 != second.Kafka.ValueSHA256 {
		t.Fatal("an already compact event must retain its existing identity hash")
	}
}

func TestPersistenceFailureNeverCommits(t *testing.T) {
	persister := &fakePersister{err: errors.New("writer unavailable")}
	committer := &fakeCommitter{}
	processor := newTestProcessor(t, persister, committer)

	if _, err := processor.Handle(context.Background(), validRiskRecord()); err == nil {
		t.Fatal("expected persistence failure")
	}
	if len(committer.calls) != 0 {
		t.Fatalf("persistence failure committed offsets: %v", committer.calls)
	}
}

func TestCollisionNeverCommits(t *testing.T) {
	persister := &fakePersister{
		err: &CollisionError{EventID: "00000000-0000-5000-8000-000000000041"},
	}
	committer := &fakeCommitter{}
	processor := newTestProcessor(t, persister, committer)

	if _, err := processor.Handle(context.Background(), validRiskRecord()); err == nil {
		t.Fatal("expected collision failure")
	}
	if len(committer.calls) != 0 {
		t.Fatalf("collision committed offsets: %v", committer.calls)
	}
}

func TestPoisonValueIsSanitizedBeforePersistence(t *testing.T) {
	persister := &fakePersister{result: PersistResult{Status: "inserted"}}
	committer := &fakeCommitter{}
	processor := newTestProcessor(t, persister, committer)
	record := validRiskRecord()
	record.Value = []byte(`{"password":"must-not-be-copied"`)

	result, err := processor.Handle(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "dead_lettered" || len(committer.calls) != 1 {
		t.Fatalf("unexpected poison result: %+v commits=%v", result, committer.calls)
	}
	request := persister.requests[0]
	if request.Mode != "dead_letter" || request.ErrorCode != "invalid_json_or_envelope" {
		t.Fatalf("unexpected dead-letter request: %+v", request)
	}
	if len(request.Event) != 0 || strings.Contains(string(request.Event), "password") {
		t.Fatalf("raw poison payload escaped sanitization: %s", request.Event)
	}
	if request.EventSHA256 == "" || request.Kafka.ValueSHA256 == "" {
		t.Fatal("sanitized audit hashes are required")
	}
}

func TestTopicSchemaMismatchBecomesAuditedDeadLetter(t *testing.T) {
	persister := &fakePersister{result: PersistResult{Status: "duplicate"}}
	committer := &fakeCommitter{}
	processor := newTestProcessor(t, persister, committer)
	record := validRiskRecord()
	record.Value = []byte(strings.Replace(
		string(record.Value), `"schema_version":1`, `"schema_version":2`, 1,
	))

	if _, err := processor.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if persister.requests[0].ErrorCode != "topic_type_schema_mismatch" {
		t.Fatalf("unexpected error code: %+v", persister.requests[0])
	}
}

func TestInvalidKindPayloadBecomesAuditedDeadLetter(t *testing.T) {
	persister := &fakePersister{result: PersistResult{Status: "inserted"}}
	committer := &fakeCommitter{}
	processor := newTestProcessor(t, persister, committer)
	record := validRiskRecord()
	record.Value = []byte(strings.Replace(
		string(record.Value),
		`"payload":{"score_id":"score-1","hand_id":"hand-1","table_id":"table-1"}`,
		`"payload":{"hand_id":"hand-1","table_id":"table-1"}`,
		1,
	))

	if _, err := processor.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if persister.requests[0].ErrorCode != "invalid_event_payload_contract" {
		t.Fatalf("unexpected error code: %+v", persister.requests[0])
	}
	if len(committer.calls) != 1 {
		t.Fatal("durable sanitized contract failure must advance its offset")
	}
}

func TestCanonicalRoutesAreEightSyntheticTopics(t *testing.T) {
	routes := CanonicalSyntheticRoutes()
	if len(routes) != 8 {
		t.Fatalf("expected eight routes, got %d", len(routes))
	}
	for _, route := range routes {
		if !strings.HasPrefix(route.Topic, "poker.synthetic.") {
			t.Fatalf("non-synthetic route: %+v", route)
		}
	}
}
