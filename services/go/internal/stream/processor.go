package stream

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	InputTopic           string
	RiskScoresTopic      string
	RuleEvidenceTopic    string
	PolicyDecisionsTopic string
	RiskAlertsTopic      string
	DeadLetterTopic      string
	AllowedTenants       []string
	ReviewPolicy         risk.ReviewPolicyDefinition
	RuleRollout          *risk.RuleRolloutConfig
}

type ProcessResult struct {
	Status      string
	HandID      string
	OutputCount int
	Committed   []RecordRef
}

type Processor struct {
	config                Config
	scorer                HandScorer
	assembler             *risk.HandAssembler
	publisher             Publisher
	committer             Committer
	offsets               *OffsetTracker
	pending               map[string]map[RecordRef]struct{}
	clock                 func() time.Time
	allowedTenants        map[string]struct{}
	reviewPolicy          risk.ReviewPolicyDefinition
	policyDecisions       int64
	reviewRecommendations int64
	mandatoryReviews      int64
	metricsMu             sync.Mutex
	scopeHands            map[monitoringScope]int64
	scopePairs            map[monitoringScope]int64
	ruleEvidence          map[monitoringRuleKey]int64
}

type monitoringScope struct {
	tenantID  string
	productID string
	modelName string
	modelRun  string
	rolloutID string
}

type monitoringRuleKey struct {
	monitoringScope
	ruleID      string
	ruleVersion int
}

func NewProcessor(config Config, scorer HandScorer, assembler *risk.HandAssembler, publisher Publisher, committer Committer, clock func() time.Time) (*Processor, error) {
	if config.InputTopic == "" || config.RiskScoresTopic == "" || config.RuleEvidenceTopic == "" ||
		config.PolicyDecisionsTopic == "" || config.RiskAlertsTopic == "" || config.DeadLetterTopic == "" {
		return nil, fmt.Errorf("all stream topics are required")
	}
	if err := config.ReviewPolicy.Validate(); err != nil {
		return nil, fmt.Errorf("review policy: %w", err)
	}
	if config.RuleRollout != nil {
		if err := config.RuleRollout.Validate(); err != nil {
			return nil, fmt.Errorf("rule rollout: %w", err)
		}
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
		allowedTenants: allowed, reviewPolicy: config.ReviewPolicy,
		scopeHands:   make(map[monitoringScope]int64),
		scopePairs:   make(map[monitoringScope]int64),
		ruleEvidence: make(map[monitoringRuleKey]int64),
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
	scoreEvent, decisionEvent, alertEvent, err := risk.BuildSeparatedOutputEvents(
		result, processor.reviewPolicy)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("build outputs for hand %s: %w", event.Payload.HandID, err)
	}
	outputs, err := processor.outputRecords(
		result.RuleEvidenceEvents, scoreEvent, decisionEvent, alertEvent)
	if err != nil {
		return ProcessResult{}, err
	}
	if err := processor.publisher.Publish(ctx, outputs); err != nil {
		return ProcessResult{}, fmt.Errorf("publish outputs for hand %s: %w", event.Payload.HandID, err)
	}
	processor.recordMonitoringMetrics(result)
	processor.policyDecisions++
	switch decisionEvent.Payload.Outcome {
	case "review_recommended":
		processor.reviewRecommendations++
	case "mandatory_review":
		processor.mandatoryReviews++
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

func (processor *Processor) recordMonitoringMetrics(result *risk.ScoreResult) {
	rolloutID := "unconfigured"
	if processor.config.RuleRollout != nil {
		rolloutID = processor.config.RuleRollout.RolloutID
	}
	scope := monitoringScope{
		tenantID: result.TenantID, productID: result.ProductID,
		modelName: result.ModelName, modelRun: result.ModelRunID,
		rolloutID: rolloutID,
	}
	processor.metricsMu.Lock()
	defer processor.metricsMu.Unlock()
	processor.scopeHands[scope]++
	processor.scopePairs[scope] += int64(len(result.PairScores))
	for _, event := range result.RuleEvidenceEvents {
		key := monitoringRuleKey{
			monitoringScope: scope,
			ruleID:          event.Payload.RuleID, ruleVersion: event.Payload.RuleVersion,
		}
		processor.ruleEvidence[key]++
	}
}

func monitoringLabelValue(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	return strings.ReplaceAll(value, "\"", "\\\"")
}

func monitoringScopeLabels(scope monitoringScope) string {
	return fmt.Sprintf(
		"tenant_id=\"%s\",product_id=\"%s\",model_name=\"%s\",model_run_id=\"%s\",rule_rollout_id=\"%s\"",
		monitoringLabelValue(scope.tenantID), monitoringLabelValue(scope.productID),
		monitoringLabelValue(scope.modelName), monitoringLabelValue(scope.modelRun),
		monitoringLabelValue(scope.rolloutID),
	)
}

// PrometheusMetrics returns acknowledged Kafka output metrics. Counts advance
// only after evidence, score, decision, and optional alert publishing succeeds.
func (processor *Processor) PrometheusMetrics() string {
	processor.metricsMu.Lock()
	scopeHands := make(map[monitoringScope]int64, len(processor.scopeHands))
	scopePairs := make(map[monitoringScope]int64, len(processor.scopePairs))
	ruleEvidence := make(map[monitoringRuleKey]int64, len(processor.ruleEvidence))
	for key, value := range processor.scopeHands {
		scopeHands[key] = value
	}
	for key, value := range processor.scopePairs {
		scopePairs[key] = value
	}
	for key, value := range processor.ruleEvidence {
		ruleEvidence[key] = value
	}
	processor.metricsMu.Unlock()

	scopes := make([]monitoringScope, 0, len(scopeHands))
	for scope := range scopeHands {
		scopes = append(scopes, scope)
	}
	sort.Slice(scopes, func(left, right int) bool {
		return monitoringScopeLabels(scopes[left]) < monitoringScopeLabels(scopes[right])
	})
	rules := make([]monitoringRuleKey, 0, len(ruleEvidence))
	for key := range ruleEvidence {
		rules = append(rules, key)
	}
	sort.Slice(rules, func(left, right int) bool {
		leftKey := monitoringScopeLabels(rules[left].monitoringScope) + rules[left].ruleID + strconv.Itoa(rules[left].ruleVersion)
		rightKey := monitoringScopeLabels(rules[right].monitoringScope) + rules[right].ruleID + strconv.Itoa(rules[right].ruleVersion)
		return leftKey < rightKey
	})

	var output strings.Builder
	output.WriteString("# HELP risk_scorer_scope_hands_scored_total Kafka-acknowledged complete hands by governed scope.\n# TYPE risk_scorer_scope_hands_scored_total counter\n")
	output.WriteString("# HELP risk_scorer_scope_pairs_scored_total Kafka-acknowledged pair rows by governed scope.\n# TYPE risk_scorer_scope_pairs_scored_total counter\n")
	for _, scope := range scopes {
		labels := monitoringScopeLabels(scope)
		fmt.Fprintf(&output, "risk_scorer_scope_hands_scored_total{%s} %d\n", labels, scopeHands[scope])
		fmt.Fprintf(&output, "risk_scorer_scope_pairs_scored_total{%s} %d\n", labels, scopePairs[scope])
	}
	output.WriteString("# HELP risk_scorer_rule_evidence_total Kafka-acknowledged rule evidence by exact governed lineage.\n# TYPE risk_scorer_rule_evidence_total counter\n")
	for _, key := range rules {
		fmt.Fprintf(
			&output,
			"risk_scorer_rule_evidence_total{%s,rule_id=\"%s\",rule_version=\"%d\"} %d\n",
			monitoringScopeLabels(key.monitoringScope), monitoringLabelValue(key.ruleID),
			key.ruleVersion, ruleEvidence[key],
		)
	}
	output.WriteString("# HELP risk_scorer_rule_enabled Governed rule rollout enablement (1 enabled, 0 disabled).\n# TYPE risk_scorer_rule_enabled gauge\n")
	if processor.config.RuleRollout != nil {
		entries := append([]risk.RuleRolloutEntry(nil), processor.config.RuleRollout.Rules...)
		sort.Slice(entries, func(left, right int) bool { return entries[left].RuleID < entries[right].RuleID })
		for _, entry := range entries {
			enabled := 0
			if entry.Enabled {
				enabled = 1
			}
			fmt.Fprintf(
				&output,
				"risk_scorer_rule_enabled{rule_rollout_id=\"%s\",rule_id=\"%s\",rule_version=\"%d\",runtime=\"%s\"} %d\n",
				monitoringLabelValue(processor.config.RuleRollout.RolloutID),
				monitoringLabelValue(entry.RuleID), entry.RuleVersion,
				monitoringLabelValue(entry.Runtime), enabled,
			)
		}
	}
	return output.String()
}

func (processor *Processor) outputRecords(
	ruleEvidence []risk.RuleEvidenceEvent,
	score risk.RiskScoreEvent,
	decision risk.ReviewDecisionEvent,
	alert *risk.RiskAlertEvent,
) ([]OutputRecord, error) {
	if len(ruleEvidence) != len(score.Payload.RuleEvidenceEventIDs) {
		return nil, fmt.Errorf("rule evidence batch does not match score references")
	}
	outputs := make([]OutputRecord, 0, len(ruleEvidence)+3)
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
	if err := decision.Validate(); err != nil {
		return nil, fmt.Errorf("validate review decision: %w", err)
	}
	if decision.Payload.RiskScoreEventID != score.EventID || decision.Payload.ScoreID != score.Payload.ScoreID ||
		decision.Payload.HandID != score.Payload.HandID {
		return nil, fmt.Errorf("review decision does not reference its risk score")
	}
	decisionValue, err := json.Marshal(decision)
	if err != nil {
		return nil, fmt.Errorf("marshal review decision: %w", err)
	}
	outputs = append(outputs, OutputRecord{
		Topic: processor.config.PolicyDecisionsTopic,
		Key:   []byte(decision.Payload.HandID), Value: decisionValue,
	})
	if alert != nil {
		if alert.Payload.ReviewDecisionEventID != decision.EventID {
			return nil, fmt.Errorf("risk alert does not reference its review decision")
		}
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

type PolicyMetricsSnapshot struct {
	Decisions                  int64
	ReviewRecommendations      int64
	MandatoryReviews           int64
	ReviewRate                 float64
	MandatoryReviewRate        float64
	MinimumSampleReached       bool
	WithinConfiguredRateLimits bool
	PromotionGatePassed        bool
}

func (processor *Processor) PolicyMetrics() PolicyMetricsSnapshot {
	value := PolicyMetricsSnapshot{
		Decisions:             processor.policyDecisions,
		ReviewRecommendations: processor.reviewRecommendations,
		MandatoryReviews:      processor.mandatoryReviews,
	}
	if value.Decisions > 0 {
		value.ReviewRate = float64(value.ReviewRecommendations+value.MandatoryReviews) /
			float64(value.Decisions)
		value.MandatoryReviewRate = float64(value.MandatoryReviews) / float64(value.Decisions)
	}
	value.MinimumSampleReached = value.Decisions >= int64(
		processor.reviewPolicy.RolloutGates.MinimumDecisions)
	value.WithinConfiguredRateLimits = value.ReviewRate <=
		processor.reviewPolicy.RolloutGates.MaximumReviewRate &&
		value.MandatoryReviewRate <=
			processor.reviewPolicy.RolloutGates.MaximumMandatoryReviewRate
	value.PromotionGatePassed = value.MinimumSampleReached &&
		value.WithinConfiguredRateLimits
	return value
}
