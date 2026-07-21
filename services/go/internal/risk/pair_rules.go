package risk

import (
	"fmt"
	"math"
	"time"
)

// PairRuleDefinition is the governed metadata for one inference-safe rule.
// Rules are observations only; they are never blended into model probability.
type PairRuleDefinition struct {
	RuleID               string  `json:"rule_id"`
	RuleVersion          int     `json:"rule_version"`
	RuleOwner            string  `json:"rule_owner"`
	Description          string  `json:"description"`
	EffectiveFrom        string  `json:"effective_from"`
	Severity             string  `json:"severity"`
	FeatureGroup         string  `json:"feature_group"`
	FeatureName          string  `json:"feature_name"`
	Operator             string  `json:"operator"`
	Threshold            float64 `json:"threshold"`
	RawScoreMultiplier   float64 `json:"raw_score_multiplier"`
	BenchmarkWeight      float64 `json:"benchmark_weight"`
	BenchmarkAggregation string  `json:"benchmark_aggregation"`
}

var pairRuleDefinitions = []PairRuleDefinition{
	{
		RuleID: "pair.one-folded-other-won", RuleVersion: 1, RuleOwner: "risk-analytics",
		Description:   "One player folded in the current hand while the other won.",
		EffectiveFrom: "2026-07-21T00:00:00Z", Severity: "medium", FeatureGroup: "current_hand",
		FeatureName: "one_folded_other_won", Operator: "eq", Threshold: 1,
		RawScoreMultiplier: 100, BenchmarkWeight: 0.20, BenchmarkAggregation: "add",
	},
	{
		RuleID: "pair.same-device", RuleVersion: 1, RuleOwner: "trust-platform",
		Description:   "Both players were linked to the same device in event-time context.",
		EffectiveFrom: "2026-07-21T00:00:00Z", Severity: "high", FeatureGroup: "context",
		FeatureName: "same_device", Operator: "eq", Threshold: 1,
		RawScoreMultiplier: 100, BenchmarkWeight: 0.20, BenchmarkAggregation: "add",
	},
	{
		RuleID: "pair.same-network", RuleVersion: 1, RuleOwner: "trust-platform",
		Description:   "Both players were linked to the same network cluster in event-time context.",
		EffectiveFrom: "2026-07-21T00:00:00Z", Severity: "medium", FeatureGroup: "context",
		FeatureName: "same_network", Operator: "eq", Threshold: 1,
		RawScoreMultiplier: 100, BenchmarkWeight: 0.20, BenchmarkAggregation: "add",
	},
	{
		RuleID: "pair.outcome-asymmetry", RuleVersion: 1, RuleOwner: "risk-analytics",
		Description:   "Prior-only pair winnings are asymmetric.",
		EffectiveFrom: "2026-07-21T00:00:00Z", Severity: "low", FeatureGroup: "pair_history",
		FeatureName: "outcome_asymmetry", Operator: "gt", Threshold: 0,
		RawScoreMultiplier: 100, BenchmarkWeight: 0.15, BenchmarkAggregation: "add",
	},
	{
		RuleID: "pair.a-fold-b-win-rate", RuleVersion: 1, RuleOwner: "risk-analytics",
		Description:   "Prior-only rate at which player A folded and player B won is non-zero.",
		EffectiveFrom: "2026-07-21T00:00:00Z", Severity: "medium", FeatureGroup: "pair_history",
		FeatureName: "a_fold_b_win_rate", Operator: "gt", Threshold: 0,
		RawScoreMultiplier: 100, BenchmarkWeight: 0.25, BenchmarkAggregation: "max_directional",
	},
	{
		RuleID: "pair.b-fold-a-win-rate", RuleVersion: 1, RuleOwner: "risk-analytics",
		Description:   "Prior-only rate at which player B folded and player A won is non-zero.",
		EffectiveFrom: "2026-07-21T00:00:00Z", Severity: "medium", FeatureGroup: "pair_history",
		FeatureName: "b_fold_a_win_rate", Operator: "gt", Threshold: 0,
		RawScoreMultiplier: 100, BenchmarkWeight: 0.25, BenchmarkAggregation: "max_directional",
	},
}

// PairRuleDefinitions returns a defensive copy in deterministic order.
func PairRuleDefinitions() []PairRuleDefinition {
	return append([]PairRuleDefinition(nil), pairRuleDefinitions...)
}

func pairRuleSignals(event PairFeatureEvent) (map[string]float64, error) {
	groups := map[string]map[string]any{
		"current_hand": event.Payload.CurrentHand,
		"context":      event.Payload.Context,
		"pair_history": event.Payload.PairHistory,
	}
	values := make(map[string]float64, len(pairRuleDefinitions))
	for _, definition := range pairRuleDefinitions {
		group := groups[definition.FeatureGroup]
		value, exists := group[definition.FeatureName]
		if !exists {
			return nil, fmt.Errorf("rule %s requires %s.%s", definition.RuleID, definition.FeatureGroup, definition.FeatureName)
		}
		number, err := numericValue(value)
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) || number < 0 || number > 1 {
			return nil, fmt.Errorf("rule %s feature must be numeric or boolean, finite, and in [0, 1]", definition.RuleID)
		}
		values[definition.FeatureName] = number
	}
	return values, nil
}

func pairRuleFires(definition PairRuleDefinition, value float64) bool {
	if definition.Operator == "eq" {
		return value == definition.Threshold
	}
	return value > definition.Threshold
}

// RulesOnlyPairScore exactly reproduces the existing Python benchmark formula.
func RulesOnlyPairScore(event PairFeatureEvent) (float64, error) {
	values, err := pairRuleSignals(event)
	if err != nil {
		return 0, err
	}
	score := 0.20*values["one_folded_other_won"] +
		0.20*values["same_device"] +
		0.20*values["same_network"] +
		0.15*values["outcome_asymmetry"] +
		0.25*math.Max(values["a_fold_b_win_rate"], values["b_fold_a_win_rate"])
	return math.Max(0, math.Min(1, score)), nil
}

// EvaluatePairRules emits independent observations for fired rules. It
// performs no I/O and does not receive a model probability.
func EvaluatePairRules(event PairFeatureEvent, emittedAt time.Time) ([]RuleEvidenceEvent, error) {
	return EvaluatePairRulesWithEnabled(event, emittedAt, nil)
}

// EvaluatePairRulesWithEnabled evaluates only explicitly enabled rules. A nil
// map preserves the legacy all-enabled behavior. The filter can only remove
// evidence; it is never passed to model inference or calibration.
func EvaluatePairRulesWithEnabled(event PairFeatureEvent, emittedAt time.Time, enabled map[string]bool) ([]RuleEvidenceEvent, error) {
	if err := event.Validate(ruleFeatureVersion); err != nil {
		return nil, err
	}
	values, err := pairRuleSignals(event)
	if err != nil {
		return nil, err
	}
	evidenceEvents := make([]RuleEvidenceEvent, 0, len(pairRuleDefinitions))
	for _, definition := range pairRuleDefinitions {
		if enabled != nil && !enabled[definition.RuleID] {
			continue
		}
		observed := values[definition.FeatureName]
		if !pairRuleFires(definition, observed) {
			continue
		}
		evidence, err := BuildRuleEvidenceEvent(RuleEvidenceInput{
			TenantID: event.TenantID, ProductID: event.ProductID,
			DatasetID: event.DatasetID, DatasetSplit: event.DatasetSplit,
			TraceID: event.TraceID, RuleID: definition.RuleID,
			RuleVersion: definition.RuleVersion, RuleOwner: definition.RuleOwner,
			EntityType: "pair", EntityKey: event.Payload.PairKey, HandID: event.Payload.HandID,
			ObservationRevision: event.Payload.SnapshotRevision,
			Severity:            definition.Severity, RawScore: observed * definition.RawScoreMultiplier,
			Evidence: map[string]any{
				"feature_group": definition.FeatureGroup, "feature_name": definition.FeatureName,
				"observed_value": observed, "operator": definition.Operator,
				"threshold": definition.Threshold, "benchmark_weight": definition.BenchmarkWeight,
				"benchmark_aggregation":        definition.BenchmarkAggregation,
				"source_pair_feature_event_id": event.EventID,
				"snapshot_revision":            event.Payload.SnapshotRevision,
			},
			EffectiveAt: event.Payload.PlayedAt, EmittedAt: emittedAt.UTC().Format(time.RFC3339Nano),
			FeatureDefinitionVersion: event.Payload.FeatureDefinitionVersion,
		})
		if err != nil {
			return nil, fmt.Errorf("build evidence for %s: %w", definition.RuleID, err)
		}
		evidenceEvents = append(evidenceEvents, evidence)
	}
	return evidenceEvents, nil
}
