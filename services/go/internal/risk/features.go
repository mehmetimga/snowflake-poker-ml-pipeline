package risk

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const pairFeatureEventType = "poker.pair-features.computed"

var forbiddenInferenceFields = map[string]struct{}{
	"collusion_group_id": {},
	"collusion_pair_id":  {},
	"collusion_scenario": {},
	"is_collusive":       {},
	"is_suspicious":      {},
	"label":              {},
	"label_available_at": {},
	"target":             {},
}

type PairFeatureEvent struct {
	EventID              string              `json:"event_id"`
	EventType            string              `json:"event_type"`
	SchemaVersion        int                 `json:"schema_version"`
	TenantID             string              `json:"tenant_id"`
	ProductID            string              `json:"product_id"`
	DatasetID            string              `json:"dataset_id"`
	DatasetSplit         string              `json:"dataset_split"`
	OccurredAt           string              `json:"occurred_at"`
	EmittedAt            string              `json:"emitted_at"`
	TraceID              string              `json:"trace_id"`
	Payload              PairFeaturePayload  `json:"payload"`
	UpstreamRuleEvidence []RuleEvidenceEvent `json:"upstream_rule_evidence,omitempty"`
}

type PairFeaturePayload struct {
	HandID                      string         `json:"hand_id"`
	TableID                     string         `json:"table_id"`
	PlayedAt                    string         `json:"played_at"`
	PairKey                     string         `json:"pair_key"`
	PlayerA                     string         `json:"player_a"`
	PlayerB                     string         `json:"player_b"`
	NumPlayers                  int            `json:"num_players"`
	SourceHandEventID           string         `json:"source_hand_event_id"`
	SourcePlayerContextEventIDA string         `json:"source_player_context_event_id_a"`
	SourcePlayerContextEventIDB string         `json:"source_player_context_event_id_b"`
	SourceRevisionA             int            `json:"source_revision_a"`
	SourceRevisionB             int            `json:"source_revision_b"`
	ContextStatusA              string         `json:"context_status_a"`
	ContextStatusB              string         `json:"context_status_b"`
	ContextVersionA             *int           `json:"context_version_a"`
	ContextVersionB             *int           `json:"context_version_b"`
	SnapshotRevision            int            `json:"snapshot_revision"`
	FeatureDefinitionVersion    string         `json:"feature_definition_version"`
	CurrentHand                 map[string]any `json:"current_hand"`
	Context                     map[string]any `json:"context"`
	UserHistoryA                map[string]any `json:"user_history_a"`
	UserHistoryB                map[string]any `json:"user_history_b"`
	PairHistory                 map[string]any `json:"pair_history"`
}

func (event PairFeatureEvent) Validate(expectedFeatureVersion string) error {
	if event.EventID == "" || event.TraceID == "" || event.TenantID == "" || event.ProductID == "" || event.DatasetID == "" || event.DatasetSplit == "" {
		return fmt.Errorf("pair event envelope identity is incomplete")
	}
	if event.EventType != pairFeatureEventType || event.SchemaVersion != 1 {
		return fmt.Errorf("unsupported pair event type or schema version")
	}
	payload := event.Payload
	if payload.HandID == "" || payload.TableID == "" || payload.PairKey == "" || payload.PlayerA == "" || payload.PlayerB == "" {
		return fmt.Errorf("pair payload identity is incomplete")
	}
	if payload.NumPlayers != 6 || payload.SnapshotRevision < 1 {
		return fmt.Errorf("pair payload requires six players and a positive revision")
	}
	if payload.SourceHandEventID == "" || payload.SourcePlayerContextEventIDA == "" || payload.SourcePlayerContextEventIDB == "" || payload.SourceRevisionA < 1 || payload.SourceRevisionB < 1 {
		return fmt.Errorf("pair source lineage is incomplete")
	}
	playedAt, err := time.Parse(time.RFC3339Nano, payload.PlayedAt)
	if err != nil {
		return fmt.Errorf("invalid played_at: %w", err)
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, event.OccurredAt)
	if err != nil || !occurredAt.Equal(playedAt) {
		return fmt.Errorf("occurred_at must be a valid timestamp equal to played_at")
	}
	emittedAt, err := time.Parse(time.RFC3339Nano, event.EmittedAt)
	if err != nil {
		return fmt.Errorf("invalid emitted_at: %w", err)
	}
	if emittedAt.Before(occurredAt) {
		return fmt.Errorf("pair-feature emitted_at cannot precede occurred_at")
	}
	if payload.PlayerA >= payload.PlayerB || payload.PairKey != payload.PlayerA+":"+payload.PlayerB {
		return fmt.Errorf("pair endpoints are not in canonical order")
	}
	validStatus := func(value string) bool {
		return value == "matched" || value == "matched_late" || value == "missing" || value == "corrected"
	}
	if !validStatus(payload.ContextStatusA) || !validStatus(payload.ContextStatusB) {
		return fmt.Errorf("invalid context join status")
	}
	for _, statusAndVersion := range []struct {
		status  string
		version *int
	}{{payload.ContextStatusA, payload.ContextVersionA}, {payload.ContextStatusB, payload.ContextVersionB}} {
		if (statusAndVersion.status == "missing") != (statusAndVersion.version == nil) {
			return fmt.Errorf("missing context status and context version disagree")
		}
	}
	if payload.FeatureDefinitionVersion != expectedFeatureVersion {
		return fmt.Errorf("feature definition mismatch: expected %s, got %s", expectedFeatureVersion, payload.FeatureDefinitionVersion)
	}
	for name, group := range map[string]map[string]any{
		"current_hand":   payload.CurrentHand,
		"context":        payload.Context,
		"user_history_a": payload.UserHistoryA,
		"user_history_b": payload.UserHistoryB,
		"pair_history":   payload.PairHistory,
	} {
		if group == nil {
			return fmt.Errorf("feature group %s is missing", name)
		}
		if err := rejectPrivateFields(group); err != nil {
			return err
		}
	}
	if len(event.UpstreamRuleEvidence) > 32 {
		return fmt.Errorf("upstream rule evidence must contain at most 32 records")
	}
	seenEvidence := make(map[string]struct{}, len(event.UpstreamRuleEvidence))
	for _, evidence := range event.UpstreamRuleEvidence {
		if err := evidence.Validate(); err != nil {
			return fmt.Errorf("invalid upstream rule evidence: %w", err)
		}
		if _, duplicate := seenEvidence[evidence.EventID]; duplicate {
			return fmt.Errorf("upstream rule-evidence references must be unique")
		}
		seenEvidence[evidence.EventID] = struct{}{}
		if evidence.TenantID != event.TenantID || evidence.ProductID != event.ProductID ||
			evidence.DatasetID != event.DatasetID || evidence.DatasetSplit != event.DatasetSplit ||
			evidence.TraceID != event.TraceID || evidence.Payload.EntityType != "pair" ||
			evidence.Payload.EntityKey != payload.PairKey || evidence.Payload.HandID != payload.HandID ||
			evidence.Payload.ObservationRevision != payload.SnapshotRevision ||
			evidence.Payload.EffectiveAt != payload.PlayedAt ||
			evidence.Payload.FeatureDefinitionVersion != payload.FeatureDefinitionVersion {
			return fmt.Errorf("upstream rule evidence does not match pair snapshot")
		}
	}
	return nil
}

func (event PairFeatureEvent) Flatten(expectedFeatureVersion string) (map[string]any, error) {
	if err := event.Validate(expectedFeatureVersion); err != nil {
		return nil, err
	}
	features := map[string]any{
		"context_status_a": event.Payload.ContextStatusA,
		"context_status_b": event.Payload.ContextStatusB,
	}
	for prefix, group := range map[string]map[string]any{
		"current_": event.Payload.CurrentHand,
		"context_": event.Payload.Context,
		"user_a_":  event.Payload.UserHistoryA,
		"user_b_":  event.Payload.UserHistoryB,
		"pair_":    event.Payload.PairHistory,
	} {
		for key, value := range group {
			features[prefix+key] = value
		}
	}
	return features, nil
}

func rejectPrivateFields(value map[string]any) error {
	for key, child := range value {
		if _, forbidden := forbiddenInferenceFields[strings.ToLower(key)]; forbidden {
			return fmt.Errorf("private inference field %q is forbidden", key)
		}
		if nested, ok := child.(map[string]any); ok {
			if err := rejectPrivateFields(nested); err != nil {
				return err
			}
		}
	}
	return nil
}

func (contract PreprocessingContract) Transform(features map[string]any) ([]float32, error) {
	row := make([]float32, 0, len(contract.OutputColumns))
	for _, column := range contract.NumericColumns {
		value, ok := features[column]
		if !ok || value == nil {
			row = append(row, float32(contract.NumericFillValues[column]))
			continue
		}
		number, err := numericValue(value)
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
			row = append(row, float32(contract.NumericFillValues[column]))
			continue
		}
		row = append(row, float32(number))
	}
	for _, column := range contract.CategoricalColumns {
		category := missingCategory
		if value, ok := features[column]; ok && value != nil {
			category = fmt.Sprint(value)
		}
		known := contract.CategoricalValues[column]
		if !containsString(known[:len(known)-1], category) {
			category = unknownCategory
		}
		for _, candidate := range known {
			if category == candidate {
				row = append(row, 1)
			} else {
				row = append(row, 0)
			}
		}
	}
	if len(row) != len(contract.OutputColumns) {
		return nil, fmt.Errorf("preprocessing produced %d features; expected %d", len(row), len(contract.OutputColumns))
	}
	return row, nil
}

func numericValue(value any) (float64, error) {
	switch typed := value.(type) {
	case bool:
		if typed {
			return 1, nil
		}
		return 0, nil
	case json.Number:
		return typed.Float64()
	case float64:
		return typed, nil
	case float32:
		return float64(typed), nil
	case int:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case int32:
		return float64(typed), nil
	case uint:
		return float64(typed), nil
	case uint64:
		return float64(typed), nil
	case string:
		return strconv.ParseFloat(typed, 64)
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", value)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func sortedEvents(events []PairFeatureEvent) []PairFeatureEvent {
	output := append([]PairFeatureEvent(nil), events...)
	sort.Slice(output, func(left, right int) bool {
		return output[left].Payload.PairKey < output[right].Payload.PairKey
	})
	return output
}
