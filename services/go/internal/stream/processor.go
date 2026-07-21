package stream

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/risk"
)

type InputRecord struct {
	RecordRef
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

type OutputRecord struct {
	Topic string
	Key   []byte
	Value []byte
}

type Publisher interface {
	Publish(context.Context, []OutputRecord) error
}

type Committer interface {
	Commit(context.Context, []RecordRef) error
}

type HandScorer interface {
	ScoreHand(context.Context, []risk.PairFeatureEvent) (*risk.ScoreResult, error)
	Contract() risk.ScoringContract
}

type Config struct {
	InputTopic        string
	RiskScoresTopic   string
	RuleEvidenceTopic string
	RiskAlertsTopic   string
	DeadLetterTopic   string
	AllowedTenants    []string
}

type ProcessResult struct {
	Status      string
	HandID      string
	OutputCount int
	Committed   []RecordRef
}

type Processor struct {
	config         Config
	scorer         HandScorer
	assembler      *risk.HandAssembler
	publisher      Publisher
	committer      Committer
	offsets        *OffsetTracker
	pending        map[string]map[RecordRef]struct{}
	clock          func() time.Time
	allowedTenants map[string]struct{}
}

func NewProcessor(config Config, scorer HandScorer, assembler *risk.HandAssembler, publisher Publisher, committer Committer, clock func() time.Time) (*Processor, error) {
	if config.InputTopic == "" || config.RiskScoresTopic == "" || config.RuleEvidenceTopic == "" || config.RiskAlertsTopic == "" || config.DeadLetterTopic == "" {
		return nil, fmt.Errorf("all stream topics are required")
	}
	if scorer == nil || assembler == nil || publisher == nil || committer == nil {
		return nil, fmt.Errorf("scorer, assembler, publisher, and committer are required")
	}
	if clock == nil {
		clock = time.Now
	}
	allowed := make(map[string]struct{}, len(config.AllowedTenants))
	for _, tenant := range config.AllowedTenants {
		if tenant == "" {
			return nil, fmt.Errorf("allowed tenant IDs cannot be empty")
		}
		allowed[tenant] = struct{}{}
	}
	return &Processor{
		config: config, scorer: scorer, assembler: assembler, publisher: publisher,
		committer: committer, offsets: NewOffsetTracker(),
		pending: make(map[string]map[RecordRef]struct{}), clock: clock,
		allowedTenants: allowed,
	}, nil
}

func (processor *Processor) Handle(ctx context.Context, record InputRecord) (ProcessResult, error) {
	processor.offsets.Observe(record.RecordRef)
	if record.Topic != processor.config.InputTopic {
		return processor.deadLetter(ctx, record, "unexpected_topic", fmt.Errorf("expected %s", processor.config.InputTopic))
	}
	var event risk.PairFeatureEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return processor.deadLetter(ctx, record, "invalid_json", err)
	}
	featureVersion := processor.scorer.Contract().FeatureDefinitionVersion
	if err := event.Validate(featureVersion); err != nil {
		return processor.deadLetter(ctx, record, "invalid_pair_feature", err)
	}
	if len(processor.allowedTenants) > 0 {
		if _, ok := processor.allowedTenants[event.TenantID]; !ok {
			return processor.deadLetter(ctx, record, "unauthorized_tenant", fmt.Errorf("tenant is not authorized"))
		}
	}
	if string(record.Key) != event.Payload.PairKey {
		return processor.deadLetter(ctx, record, "invalid_partition_key", fmt.Errorf("expected pair key %s", event.Payload.PairKey))
	}
	handKey := event.TenantID + "\x00" + event.DatasetID + "\x00" + event.DatasetSplit + "\x00" + event.Payload.HandID
	pairs, status, err := processor.assembler.AddDetailed(event, featureVersion, processor.clock())
	if err != nil {
		return processor.deadLetter(ctx, record, "assembly_rejected", err)
	}
	switch status {
	case risk.AssemblyDuplicate, risk.AssemblyStale:
		processor.offsets.MarkProcessed(record.RecordRef)
		committed, err := processor.commitReady(ctx)
		return ProcessResult{Status: string(status), HandID: event.Payload.HandID, Committed: committed}, err
	case risk.AssemblyIncomplete:
		processor.addPending(handKey, record.RecordRef)
		committed, err := processor.commitReady(ctx)
		return ProcessResult{Status: string(status), HandID: event.Payload.HandID, Committed: committed}, err
	case risk.AssemblyComplete:
		processor.addPending(handKey, record.RecordRef)
	default:
		return ProcessResult{}, fmt.Errorf("unknown assembly status %q", status)
	}

	result, err := processor.scorer.ScoreHand(ctx, pairs)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("score hand %s: %w", event.Payload.HandID, err)
	}
	scoreEvent, alertEvent, err := risk.BuildOutputEvents(result)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("build outputs for hand %s: %w", event.Payload.HandID, err)
	}
	outputs, err := processor.outputRecords(result.RuleEvidenceEvents, scoreEvent, alertEvent)
	if err != nil {
		return ProcessResult{}, err
	}
	if err := processor.publisher.Publish(ctx, outputs); err != nil {
		return ProcessResult{}, fmt.Errorf("publish outputs for hand %s: %w", event.Payload.HandID, err)
	}
	refs := processor.pendingRefs(handKey)
	processor.offsets.MarkProcessed(refs...)
	delete(processor.pending, handKey)
	committed, err := processor.commitReady(ctx)
	return ProcessResult{
		Status: string(status), HandID: event.Payload.HandID,
		OutputCount: len(outputs), Committed: committed,
	}, err
}

func (processor *Processor) outputRecords(ruleEvidence []risk.RuleEvidenceEvent, score risk.RiskScoreEvent, alert *risk.RiskAlertEvent) ([]OutputRecord, error) {
	if len(ruleEvidence) != len(score.Payload.RuleEvidenceEventIDs) {
		return nil, fmt.Errorf("rule evidence batch does not match score references")
	}
	outputs := make([]OutputRecord, 0, len(ruleEvidence)+2)
	for index, event := range ruleEvidence {
		if err := event.Validate(); err != nil {
			return nil, fmt.Errorf("validate rule evidence: %w", err)
		}
		if event.EventID != score.Payload.RuleEvidenceEventIDs[index] {
			return nil, fmt.Errorf("rule evidence order does not match score references")
		}
		value, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("marshal rule evidence: %w", err)
		}
		key := event.Payload.EntityType + ":" + event.Payload.EntityKey
		outputs = append(outputs, OutputRecord{
			Topic: processor.config.RuleEvidenceTopic, Key: []byte(key), Value: value,
		})
	}
	scoreValue, err := json.Marshal(score)
	if err != nil {
		return nil, fmt.Errorf("marshal risk score: %w", err)
	}
	outputs = append(outputs, OutputRecord{Topic: processor.config.RiskScoresTopic, Key: []byte(score.Payload.HandID), Value: scoreValue})
	if alert != nil {
		alertValue, err := json.Marshal(alert)
		if err != nil {
			return nil, fmt.Errorf("marshal risk alert: %w", err)
		}
		outputs = append(outputs, OutputRecord{Topic: processor.config.RiskAlertsTopic, Key: []byte(alert.Payload.HandID), Value: alertValue})
	}
	return outputs, nil
}

func (processor *Processor) deadLetter(ctx context.Context, record InputRecord, code string, cause error) (ProcessResult, error) {
	now := processor.clock().UTC().Format(time.RFC3339Nano)
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d:%s", record.Topic, record.Partition, record.Offset, code)))
	eventID := hex.EncodeToString(digest[:16])
	payload := map[string]any{
		"event_id": eventID, "event_type": "poker.pipeline.dead-lettered", "schema_version": 1,
		"occurred_at": now, "emitted_at": now,
		"payload": map[string]any{
			"source_topic": record.Topic, "source_partition": record.Partition,
			"source_offset": record.Offset, "source_key": string(record.Key),
			"error_code": code, "error": cause.Error(),
			"raw_value_base64": base64.StdEncoding.EncodeToString(record.Value),
		},
	}
	value, err := json.Marshal(payload)
	if err != nil {
		return ProcessResult{}, err
	}
	if err := processor.publisher.Publish(ctx, []OutputRecord{{
		Topic: processor.config.DeadLetterTopic, Key: []byte(eventID), Value: value,
	}}); err != nil {
		return ProcessResult{}, fmt.Errorf("publish dead letter: %w", err)
	}
	processor.offsets.MarkProcessed(record.RecordRef)
	committed, err := processor.commitReady(ctx)
	return ProcessResult{Status: "dead_lettered", OutputCount: 1, Committed: committed}, err
}

func (processor *Processor) addPending(handKey string, record RecordRef) {
	if processor.pending[handKey] == nil {
		processor.pending[handKey] = make(map[RecordRef]struct{})
	}
	processor.pending[handKey][record] = struct{}{}
}

func (processor *Processor) pendingRefs(handKey string) []RecordRef {
	refs := make([]RecordRef, 0, len(processor.pending[handKey]))
	for ref := range processor.pending[handKey] {
		refs = append(refs, ref)
	}
	return refs
}

func (processor *Processor) commitReady(ctx context.Context) ([]RecordRef, error) {
	ready := processor.offsets.Ready()
	if len(ready) == 0 {
		return nil, nil
	}
	if err := processor.committer.Commit(ctx, ready); err != nil {
		return nil, fmt.Errorf("commit safe offsets: %w", err)
	}
	processor.offsets.Acknowledge(ready)
	return ready, nil
}

func (processor *Processor) PendingHands() int {
	return len(processor.pending)
}
