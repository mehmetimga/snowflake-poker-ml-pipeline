package risk

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
)

const (
	RuleEvidenceEventType = "poker.rule-evidence.recorded"
	RuleEvidenceTopic     = "poker.rule-evidence.v1"
	ruleFeatureVersion    = "pair-features-v1"
)

var (
	ruleIDPattern               = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]+$`)
	uuidPattern                 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	forbiddenRuleEvidenceFields = map[string]struct{}{
		"alert": {}, "calibrated_probability": {}, "challenge_label": {},
		"challenge_labels": {}, "collusion_group_id": {}, "collusion_pair_id": {},
		"collusion_scenario": {}, "decision": {}, "decision_policy": {},
		"decision_policy_version": {}, "decision_threshold": {}, "final_probability": {},
		"hand_risk_probability": {}, "is_collusive": {}, "is_suspicious": {},
		"label": {}, "label_available_at": {}, "labels": {}, "model_probability": {},
		"policy_action": {}, "private_challenge": {}, "private_challenge_label": {},
		"private_challenge_labels": {}, "probability": {}, "raw_probability": {},
		"review_required": {}, "risk_probability": {}, "scenario_name": {}, "target": {},
	}
)

type RuleEvidencePayload struct {
	RuleEventID              string         `json:"rule_event_id"`
	RuleID                   string         `json:"rule_id"`
	RuleVersion              int            `json:"rule_version"`
	RuleOwner                string         `json:"rule_owner"`
	EntityType               string         `json:"entity_type"`
	EntityKey                string         `json:"entity_key"`
	HandID                   string         `json:"hand_id"`
	ObservationRevision      int            `json:"observation_revision"`
	Severity                 string         `json:"severity"`
	RawScore                 float64        `json:"raw_score"`
	Evidence                 map[string]any `json:"evidence"`
	EffectiveAt              string         `json:"effective_at"`
	FeatureDefinitionVersion string         `json:"feature_definition_version"`
}

type RuleEvidenceEvent struct {
	EventID       string              `json:"event_id"`
	EventType     string              `json:"event_type"`
	SchemaVersion int                 `json:"schema_version"`
	TenantID      string              `json:"tenant_id"`
	ProductID     string              `json:"product_id"`
	DatasetID     string              `json:"dataset_id"`
	DatasetSplit  string              `json:"dataset_split"`
	OccurredAt    string              `json:"occurred_at"`
	EmittedAt     string              `json:"emitted_at"`
	TraceID       string              `json:"trace_id"`
	Payload       RuleEvidencePayload `json:"payload"`
}

type RuleEvidenceInput struct {
	TenantID                 string
	ProductID                string
	DatasetID                string
	DatasetSplit             string
	TraceID                  string
	RuleID                   string
	RuleVersion              int
	RuleOwner                string
	EntityType               string
	EntityKey                string
	HandID                   string
	ObservationRevision      int
	Severity                 string
	RawScore                 float64
	Evidence                 map[string]any
	EffectiveAt              string
	EmittedAt                string
	FeatureDefinitionVersion string
}

func BuildRuleEvidenceEvent(input RuleEvidenceInput) (RuleEvidenceEvent, error) {
	if input.EmittedAt == "" {
		input.EmittedAt = input.EffectiveAt
	}
	if input.FeatureDefinitionVersion == "" {
		input.FeatureDefinitionVersion = ruleFeatureVersion
	}
	if input.ObservationRevision == 0 {
		input.ObservationRevision = 1
	}
	effectiveAt, err := time.Parse(time.RFC3339Nano, input.EffectiveAt)
	if err != nil {
		return RuleEvidenceEvent{}, fmt.Errorf("invalid effective_at: %w", err)
	}
	eventID := stableRuleEventID(input, effectiveAt)
	event := RuleEvidenceEvent{
		EventID: eventID, EventType: RuleEvidenceEventType, SchemaVersion: 1,
		TenantID: input.TenantID, ProductID: input.ProductID,
		DatasetID: input.DatasetID, DatasetSplit: input.DatasetSplit,
		OccurredAt: input.EffectiveAt, EmittedAt: input.EmittedAt, TraceID: input.TraceID,
		Payload: RuleEvidencePayload{
			RuleEventID: eventID, RuleID: input.RuleID, RuleVersion: input.RuleVersion,
			RuleOwner: input.RuleOwner, EntityType: input.EntityType, EntityKey: input.EntityKey,
			HandID: input.HandID, ObservationRevision: input.ObservationRevision,
			Severity: input.Severity, RawScore: input.RawScore,
			Evidence: input.Evidence, EffectiveAt: input.EffectiveAt,
			FeatureDefinitionVersion: input.FeatureDefinitionVersion,
		},
	}
	if err := event.Validate(); err != nil {
		return RuleEvidenceEvent{}, err
	}
	return event, nil
}

func (event RuleEvidenceEvent) Validate() error {
	if event.EventType != RuleEvidenceEventType || event.SchemaVersion != 1 {
		return fmt.Errorf("unsupported rule-evidence event type or schema version")
	}
	if event.EventID == "" || event.TraceID == "" || event.TenantID == "" ||
		event.ProductID == "" || event.DatasetID == "" || event.DatasetSplit == "" {
		return fmt.Errorf("rule-evidence envelope identity is incomplete")
	}
	payload := event.Payload
	if payload.RuleEventID != event.EventID {
		return fmt.Errorf("rule event_id must equal payload rule_event_id")
	}
	if !ruleIDPattern.MatchString(payload.RuleID) || payload.RuleVersion < 1 ||
		payload.RuleOwner == "" || payload.EntityKey == "" || payload.HandID == "" || payload.ObservationRevision < 1 {
		return fmt.Errorf("rule-evidence payload identity is incomplete")
	}
	if !containsRuleValue([]string{"pair", "player", "hand", "session", "account"}, payload.EntityType) {
		return fmt.Errorf("invalid rule entity type %q", payload.EntityType)
	}
	if payload.EntityType == "pair" {
		players := strings.Split(payload.EntityKey, ":")
		if len(players) != 2 || players[0] == "" || players[1] == "" || players[0] >= players[1] {
			return fmt.Errorf("pair rule entity_key must use canonical player order")
		}
	}
	if !containsRuleValue([]string{"info", "low", "medium", "high", "critical"}, payload.Severity) {
		return fmt.Errorf("invalid rule severity %q", payload.Severity)
	}
	if math.IsNaN(payload.RawScore) || math.IsInf(payload.RawScore, 0) || payload.RawScore < 0 || payload.RawScore > 100 {
		return fmt.Errorf("rule raw_score must be between zero and 100")
	}
	if payload.FeatureDefinitionVersion != ruleFeatureVersion {
		return fmt.Errorf("unsupported rule feature definition version")
	}
	if len(payload.Evidence) == 0 {
		return fmt.Errorf("structured rule evidence is required")
	}
	if err := rejectRuleEvidenceFields(payload.Evidence, "$.evidence"); err != nil {
		return err
	}
	effectiveAt, err := time.Parse(time.RFC3339Nano, payload.EffectiveAt)
	if err != nil {
		return fmt.Errorf("invalid effective_at: %w", err)
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, event.OccurredAt)
	if err != nil || !occurredAt.Equal(effectiveAt) {
		return fmt.Errorf("rule occurred_at must equal evidence effective_at")
	}
	emittedAt, err := time.Parse(time.RFC3339Nano, event.EmittedAt)
	if err != nil {
		return fmt.Errorf("invalid emitted_at: %w", err)
	}
	if emittedAt.Before(occurredAt) {
		return fmt.Errorf("rule emitted_at cannot precede occurred_at")
	}
	input := RuleEvidenceInput{
		TenantID: event.TenantID, ProductID: event.ProductID,
		DatasetID: event.DatasetID, DatasetSplit: event.DatasetSplit,
		RuleID: payload.RuleID, RuleVersion: payload.RuleVersion,
		EntityType: payload.EntityType, EntityKey: payload.EntityKey,
		HandID: payload.HandID, ObservationRevision: payload.ObservationRevision,
		FeatureDefinitionVersion: payload.FeatureDefinitionVersion,
	}
	if expected := stableRuleEventID(input, effectiveAt); event.EventID != expected {
		return fmt.Errorf("rule event_id is not the deterministic replay identity")
	}
	return nil
}

func rejectRuleEvidenceFields(value any, path string) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if _, forbidden := forbiddenRuleEvidenceFields[strings.ToLower(key)]; forbidden {
				return fmt.Errorf("forbidden rule-evidence field found at %s.%s", path, key)
			}
			if err := rejectRuleEvidenceFields(child, path+"."+key); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range typed {
			if err := rejectRuleEvidenceFields(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func stableRuleEventID(input RuleEvidenceInput, effectiveAt time.Time) string {
	parts := []string{
		input.TenantID, input.ProductID, input.DatasetID, input.DatasetSplit,
		input.RuleID, fmt.Sprint(input.RuleVersion), input.EntityType, input.EntityKey,
		input.HandID, fmt.Sprint(input.ObservationRevision),
		effectiveAt.UTC().Format("2006-01-02T15:04:05.000000Z"),
		input.FeatureDefinitionVersion,
	}
	return uuidV5URL(strings.Join(parts, "\x1f"))
}

func uuidV5URL(name string) string {
	namespace := [16]byte{0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	digest := sha1.New()
	_, _ = digest.Write(namespace[:])
	_, _ = digest.Write([]byte(name))
	bytes := digest.Sum(nil)[:16]
	bytes[6] = (bytes[6] & 0x0f) | 0x50
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	hexValue := hex.EncodeToString(bytes)
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexValue[0:8], hexValue[8:12], hexValue[12:16], hexValue[16:20], hexValue[20:32])
}

func containsRuleValue(values []string, candidate string) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func validateRuleEvidenceReferences(values []string) error {
	if len(values) > 256 {
		return fmt.Errorf("at most 256 rule-evidence references are allowed")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !uuidPattern.MatchString(value) {
			return fmt.Errorf("rule-evidence reference is not a UUID: %q", value)
		}
		if _, duplicate := seen[value]; duplicate {
			return fmt.Errorf("rule-evidence event references must be unique")
		}
		seen[value] = struct{}{}
	}
	return nil
}
