package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/stream"
)

const (
	KindHand           = "hand"
	KindPlayerContext  = "player_context"
	KindPairFeature    = "pair_feature"
	KindRiskScore      = "risk_score"
	KindRuleEvidence   = "rule_evidence"
	KindReviewDecision = "review_decision"
	KindRiskAlert      = "risk_alert"
	KindDeadLetter     = "dead_letter"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type Route struct {
	Kind          string
	Topic         string
	EventType     string
	SchemaVersion int
}

type Config struct {
	Routes              []Route
	AllowedTenants      []string
	ServiceBuildVersion string
}

type KafkaPosition struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	TimestampMS int64  `json:"timestamp_ms"`
	KeySHA256   string `json:"key_sha256"`
	ValueSHA256 string `json:"value_sha256"`
}

type PersistRequest struct {
	Mode                string          `json:"mode"`
	Kind                string          `json:"kind"`
	EventID             string          `json:"event_id"`
	EventType           string          `json:"event_type,omitempty"`
	SchemaVersion       int             `json:"schema_version,omitempty"`
	TenantID            string          `json:"tenant_id,omitempty"`
	ProductID           string          `json:"product_id,omitempty"`
	DatasetID           string          `json:"dataset_id,omitempty"`
	DatasetSplit        string          `json:"dataset_split,omitempty"`
	OccurredAt          string          `json:"occurred_at,omitempty"`
	EmittedAt           string          `json:"emitted_at,omitempty"`
	TraceID             string          `json:"trace_id,omitempty"`
	EventSHA256         string          `json:"event_sha256"`
	Event               json.RawMessage `json:"event,omitempty"`
	ErrorCode           string          `json:"error_code,omitempty"`
	ServiceBuildVersion string          `json:"service_build_version"`
	Kafka               KafkaPosition   `json:"kafka"`
}

type PersistResult struct {
	Status string `json:"status"`
}

type Persister interface {
	Persist(context.Context, PersistRequest) (PersistResult, error)
}

type RuntimeMetrics struct {
	InputRecords      int64
	InsertedEvents    int64
	DuplicateEvents   int64
	DeadLetters       int64
	PersistenceErrors int64
	CommitErrors      int64
	CommittedRecords  int64
}

type Processor struct {
	routes         map[string]Route
	allowedTenants map[string]struct{}
	buildVersion   string
	persister      Persister
	committer      stream.Committer
	offsets        *stream.OffsetTracker
	metricsMu      sync.Mutex
	metrics        RuntimeMetrics
}

type envelope struct {
	EventID              string          `json:"event_id"`
	EventType            string          `json:"event_type"`
	SchemaVersion        int             `json:"schema_version"`
	TenantID             string          `json:"tenant_id"`
	ProductID            string          `json:"product_id"`
	DatasetID            string          `json:"dataset_id"`
	DatasetSplit         string          `json:"dataset_split"`
	OccurredAt           string          `json:"occurred_at"`
	EmittedAt            string          `json:"emitted_at"`
	TraceID              string          `json:"trace_id"`
	Payload              json.RawMessage `json:"payload"`
	UpstreamRuleEvidence json.RawMessage `json:"upstream_rule_evidence,omitempty"`
}

type payloadIdentity struct {
	HandID              string `json:"hand_id"`
	TableID             string `json:"table_id"`
	PairKey             string `json:"pair_key"`
	ScoreID             string `json:"score_id"`
	RuleEventID         string `json:"rule_event_id"`
	DecisionID          string `json:"decision_id"`
	AlertID             string `json:"alert_id"`
	Revision            int    `json:"revision"`
	SnapshotRevision    int    `json:"snapshot_revision"`
	ObservationRevision int    `json:"observation_revision"`
	Player              struct {
		PlayerID string `json:"player_id"`
	} `json:"player"`
}

func CanonicalSyntheticRoutes() []Route {
	return []Route{
		{Kind: KindHand, Topic: "poker.synthetic.hands.raw.v1", EventType: "poker.hand.completed", SchemaVersion: 1},
		{Kind: KindPlayerContext, Topic: "poker.synthetic.hand-player-context.v2", EventType: "poker.hand-player-context.enriched", SchemaVersion: 2},
		{Kind: KindPairFeature, Topic: "poker.synthetic.pair-features.context-v2.v1", EventType: "poker.pair-features.computed", SchemaVersion: 1},
		{Kind: KindRiskScore, Topic: "poker.synthetic.risk-scores.v1", EventType: "poker.risk-score.computed", SchemaVersion: 1},
		{Kind: KindRuleEvidence, Topic: "poker.synthetic.rule-evidence.v1", EventType: "poker.rule-evidence.recorded", SchemaVersion: 1},
		{Kind: KindReviewDecision, Topic: "poker.synthetic.review-decisions.v1", EventType: "poker.review-decision.recorded", SchemaVersion: 1},
		{Kind: KindRiskAlert, Topic: "poker.synthetic.risk-alerts.v1", EventType: "poker.risk-alert.created", SchemaVersion: 1},
		{Kind: KindDeadLetter, Topic: "poker.synthetic.pipeline.dead-letter.v1"},
	}
}

func NewProcessor(config Config, persister Persister, committer stream.Committer) (*Processor, error) {
	if persister == nil || committer == nil {
		return nil, fmt.Errorf("persister and committer are required")
	}
	if strings.TrimSpace(config.ServiceBuildVersion) == "" {
		return nil, fmt.Errorf("service build version is required")
	}
	if len(config.Routes) == 0 {
		return nil, fmt.Errorf("at least one sink route is required")
	}
	routes := make(map[string]Route, len(config.Routes))
	kinds := make(map[string]struct{}, len(config.Routes))
	for _, route := range config.Routes {
		if route.Kind == "" || route.Topic == "" {
			return nil, fmt.Errorf("sink route kind and topic are required")
		}
		if !strings.HasPrefix(route.Topic, "poker.synthetic.") {
			return nil, fmt.Errorf("canonical sink topic must use poker.synthetic.*: %s", route.Topic)
		}
		if _, exists := routes[route.Topic]; exists {
			return nil, fmt.Errorf("duplicate sink topic: %s", route.Topic)
		}
		if _, exists := kinds[route.Kind]; exists {
			return nil, fmt.Errorf("duplicate sink kind: %s", route.Kind)
		}
		if route.Kind != KindDeadLetter && (route.EventType == "" || route.SchemaVersion < 1) {
			return nil, fmt.Errorf("event sink routes require type and schema")
		}
		routes[route.Topic] = route
		kinds[route.Kind] = struct{}{}
	}
	allowed := make(map[string]struct{}, len(config.AllowedTenants))
	for _, tenant := range config.AllowedTenants {
		tenant = strings.TrimSpace(tenant)
		if tenant == "" {
			return nil, fmt.Errorf("allowed tenant cannot be empty")
		}
		allowed[tenant] = struct{}{}
	}
	return &Processor{
		routes: routes, allowedTenants: allowed, buildVersion: config.ServiceBuildVersion,
		persister: persister, committer: committer, offsets: stream.NewOffsetTracker(),
	}, nil
}

func (processor *Processor) Topics() []string {
	topics := make([]string, 0, len(processor.routes))
	for topic := range processor.routes {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

func (processor *Processor) Handle(ctx context.Context, record stream.InputRecord) (stream.ProcessResult, error) {
	processor.offsets.Observe(record.RecordRef)
	processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.InputRecords++ })
	route, exists := processor.routes[record.Topic]
	if !exists {
		return processor.persistDeadLetter(ctx, record, "unexpected_topic")
	}
	if route.Kind == KindDeadLetter {
		return processor.persistDeadLetter(ctx, record, "upstream_dead_letter")
	}
	event, code := processor.validateEvent(route, record.Value)
	if code != "" {
		return processor.persistDeadLetter(ctx, record, code)
	}
	request := processor.eventRequest(route, record, event)
	result, err := processor.persister.Persist(ctx, request)
	if err != nil {
		processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.PersistenceErrors++ })
		return stream.ProcessResult{}, fmt.Errorf("persist %s event: %w", route.Kind, err)
	}
	switch result.Status {
	case "inserted":
		processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.InsertedEvents++ })
	case "duplicate":
		processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.DuplicateEvents++ })
	default:
		return stream.ProcessResult{}, fmt.Errorf("writer returned unsupported status %q", result.Status)
	}
	committed, err := processor.markAndCommit(ctx, record.RecordRef)
	if err != nil {
		return stream.ProcessResult{}, err
	}
	return stream.ProcessResult{
		Status: result.Status, OutputCount: 1, Committed: committed,
	}, nil
}

func (processor *Processor) validateEvent(route Route, value []byte) (envelope, string) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var event envelope
	if err := decoder.Decode(&event); err != nil {
		return envelope{}, "invalid_json_or_envelope"
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return envelope{}, "trailing_json"
	}
	if !uuidPattern.MatchString(event.EventID) || !uuidPattern.MatchString(event.TraceID) {
		return envelope{}, "invalid_event_identity"
	}
	if event.EventType != route.EventType || event.SchemaVersion != route.SchemaVersion {
		return envelope{}, "topic_type_schema_mismatch"
	}
	if event.TenantID == "" || event.ProductID == "" || event.DatasetID == "" || event.DatasetSplit == "" {
		return envelope{}, "incomplete_event_scope"
	}
	if len(processor.allowedTenants) > 0 {
		if _, allowed := processor.allowedTenants[event.TenantID]; !allowed {
			return envelope{}, "tenant_not_allowed"
		}
	}
	occurredAt, occurredErr := time.Parse(time.RFC3339Nano, event.OccurredAt)
	emittedAt, emittedErr := time.Parse(time.RFC3339Nano, event.EmittedAt)
	if occurredErr != nil || emittedErr != nil || emittedAt.Before(occurredAt) {
		return envelope{}, "invalid_event_time"
	}
	payload := bytes.TrimSpace(event.Payload)
	if len(payload) < 2 || payload[0] != '{' {
		return envelope{}, "invalid_event_payload"
	}
	if err := validatePayloadIdentity(route.Kind, event.Payload); err != nil {
		return envelope{}, "invalid_event_payload_contract"
	}
	return event, ""
}

func validatePayloadIdentity(kind string, raw json.RawMessage) error {
	var payload payloadIdentity
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	requireHandAndTable := func() error {
		if payload.HandID == "" || payload.TableID == "" {
			return fmt.Errorf("hand_id and table_id are required")
		}
		return nil
	}
	switch kind {
	case KindHand:
		return requireHandAndTable()
	case KindPlayerContext:
		if err := requireHandAndTable(); err != nil {
			return err
		}
		if payload.Player.PlayerID == "" || payload.Revision < 1 {
			return fmt.Errorf("player context identity and revision are required")
		}
	case KindPairFeature:
		if err := requireHandAndTable(); err != nil {
			return err
		}
		if payload.PairKey == "" || payload.SnapshotRevision < 1 {
			return fmt.Errorf("pair identity and snapshot revision are required")
		}
	case KindRiskScore:
		if err := requireHandAndTable(); err != nil {
			return err
		}
		if payload.ScoreID == "" {
			return fmt.Errorf("score identity is required")
		}
	case KindRuleEvidence:
		if payload.HandID == "" || payload.RuleEventID == "" ||
			payload.ObservationRevision < 1 {
			return fmt.Errorf("rule evidence identity and revision are required")
		}
	case KindReviewDecision:
		if err := requireHandAndTable(); err != nil {
			return err
		}
		if payload.DecisionID == "" {
			return fmt.Errorf("review decision identity is required")
		}
	case KindRiskAlert:
		if err := requireHandAndTable(); err != nil {
			return err
		}
		if payload.AlertID == "" {
			return fmt.Errorf("risk alert identity is required")
		}
	default:
		return fmt.Errorf("unsupported event kind")
	}
	return nil
}

func (processor *Processor) eventRequest(route Route, record stream.InputRecord, event envelope) PersistRequest {
	valueDigest := sha256.Sum256(record.Value)
	return PersistRequest{
		Mode: "event", Kind: route.Kind, EventID: event.EventID,
		EventType: event.EventType, SchemaVersion: event.SchemaVersion,
		TenantID: event.TenantID, ProductID: event.ProductID,
		DatasetID: event.DatasetID, DatasetSplit: event.DatasetSplit,
		OccurredAt: event.OccurredAt, EmittedAt: event.EmittedAt, TraceID: event.TraceID,
		EventSHA256: hex.EncodeToString(valueDigest[:]), Event: json.RawMessage(record.Value),
		ServiceBuildVersion: processor.buildVersion, Kafka: kafkaPosition(record),
	}
}

func (processor *Processor) persistDeadLetter(
	ctx context.Context,
	record stream.InputRecord,
	code string,
) (stream.ProcessResult, error) {
	valueDigest := sha256.Sum256(record.Value)
	identity := fmt.Sprintf(
		"poker-sink-dead-letter:%s:%s:%d:%d:%s",
		processor.buildVersion, record.Topic, record.Partition, record.Offset, code,
	)
	idDigest := sha256.Sum256([]byte(identity))
	request := PersistRequest{
		Mode: "dead_letter", Kind: KindDeadLetter,
		EventID:     hex.EncodeToString(idDigest[:16]),
		EventSHA256: hex.EncodeToString(valueDigest[:]), ErrorCode: code,
		ServiceBuildVersion: processor.buildVersion, Kafka: kafkaPosition(record),
	}
	result, err := processor.persister.Persist(ctx, request)
	if err != nil {
		processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.PersistenceErrors++ })
		return stream.ProcessResult{}, fmt.Errorf("persist sink dead letter: %w", err)
	}
	if result.Status != "inserted" && result.Status != "duplicate" {
		return stream.ProcessResult{}, fmt.Errorf("writer returned unsupported dead-letter status %q", result.Status)
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

func kafkaPosition(record stream.InputRecord) KafkaPosition {
	keyDigest := sha256.Sum256(record.Key)
	valueDigest := sha256.Sum256(record.Value)
	timestampMS := int64(0)
	if !record.Timestamp.IsZero() {
		timestampMS = record.Timestamp.UnixMilli()
	}
	return KafkaPosition{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		TimestampMS: timestampMS, KeySHA256: hex.EncodeToString(keyDigest[:]),
		ValueSHA256: hex.EncodeToString(valueDigest[:]),
	}
}

func (processor *Processor) markAndCommit(
	ctx context.Context, record stream.RecordRef,
) ([]stream.RecordRef, error) {
	processor.offsets.MarkProcessed(record)
	ready := processor.offsets.Ready()
	if len(ready) == 0 {
		return nil, nil
	}
	if err := processor.committer.Commit(ctx, ready); err != nil {
		processor.updateMetrics(func(metrics *RuntimeMetrics) { metrics.CommitErrors++ })
		return nil, fmt.Errorf("commit persisted sink offsets: %w", err)
	}
	processor.offsets.Acknowledge(ready)
	processor.updateMetrics(func(metrics *RuntimeMetrics) {
		metrics.CommittedRecords += int64(len(ready))
	})
	return ready, nil
}

func (processor *Processor) Metrics() RuntimeMetrics {
	processor.metricsMu.Lock()
	defer processor.metricsMu.Unlock()
	return processor.metrics
}

func (processor *Processor) PrometheusMetrics() string {
	metrics := processor.Metrics()
	return fmt.Sprintf(
		"# TYPE poker_sink_input_records_total counter\n"+
			"poker_sink_input_records_total %d\n"+
			"# TYPE poker_sink_inserted_events_total counter\n"+
			"poker_sink_inserted_events_total %d\n"+
			"# TYPE poker_sink_duplicate_events_total counter\n"+
			"poker_sink_duplicate_events_total %d\n"+
			"# TYPE poker_sink_dead_letters_total counter\n"+
			"poker_sink_dead_letters_total %d\n"+
			"# TYPE poker_sink_persistence_errors_total counter\n"+
			"poker_sink_persistence_errors_total %d\n"+
			"# TYPE poker_sink_commit_errors_total counter\n"+
			"poker_sink_commit_errors_total %d\n"+
			"# TYPE poker_sink_committed_records_total counter\n"+
			"poker_sink_committed_records_total %d\n",
		metrics.InputRecords,
		metrics.InsertedEvents,
		metrics.DuplicateEvents,
		metrics.DeadLetters,
		metrics.PersistenceErrors,
		metrics.CommitErrors,
		metrics.CommittedRecords,
	)
}

func (processor *Processor) updateMetrics(update func(*RuntimeMetrics)) {
	processor.metricsMu.Lock()
	defer processor.metricsMu.Unlock()
	update(&processor.metrics)
}
