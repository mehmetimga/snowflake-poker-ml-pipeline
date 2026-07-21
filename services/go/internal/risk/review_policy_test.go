package risk

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

type reviewPolicyGolden struct {
	TenantID               string `json:"tenant_id"`
	ProductID              string `json:"product_id"`
	DatasetID              string `json:"dataset_id"`
	DatasetSplit           string `json:"dataset_split"`
	TraceID                string `json:"trace_id"`
	RiskScoreEventID       string `json:"risk_score_event_id"`
	ScoreID                string `json:"score_id"`
	HandID                 string `json:"hand_id"`
	TableID                string `json:"table_id"`
	PlayedAt               string `json:"played_at"`
	DecidedAt              string `json:"decided_at"`
	ModelThresholdExceeded bool   `json:"model_threshold_exceeded"`
	RuleEvidence           []struct {
		RuleEventID string `json:"rule_event_id"`
		RuleID      string `json:"rule_id"`
		RuleVersion int    `json:"rule_version"`
	} `json:"rule_evidence"`
	Expected struct {
		DecisionID   string   `json:"decision_id"`
		Outcome      string   `json:"outcome"`
		Action       string   `json:"action"`
		ReasonCodes  []string `json:"reason_codes"`
		RuleCategory string   `json:"rule_category"`
	} `json:"expected"`
}

func loadReviewPolicyGolden(t *testing.T) reviewPolicyGolden {
	t.Helper()
	value, err := os.ReadFile("../../../../schemas/examples/review-policy-v1.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture reviewPolicyGolden
	if err := json.Unmarshal(value, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func goldenPolicyScore(t *testing.T, fixture reviewPolicyGolden) *ScoreResult {
	t.Helper()
	rule := fixture.RuleEvidence[0]
	evidence, err := BuildRuleEvidenceEvent(RuleEvidenceInput{
		TenantID: fixture.TenantID, ProductID: fixture.ProductID,
		DatasetID: fixture.DatasetID, DatasetSplit: fixture.DatasetSplit,
		TraceID: fixture.TraceID, RuleID: rule.RuleID, RuleVersion: rule.RuleVersion,
		RuleOwner: "risk-analytics", EntityType: "pair", EntityKey: "player-a:player-b",
		HandID: fixture.HandID, ObservationRevision: 1, Severity: "high", RawScore: 60,
		Evidence:    map[string]any{"window_hand_count": 5, "directional_fold_win_rate": 0.6},
		EffectiveAt: fixture.PlayedAt, EmittedAt: fixture.DecidedAt,
		FeatureDefinitionVersion: "pair-features-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.EventID != rule.RuleEventID {
		t.Fatalf("fixture rule event ID mismatch: got %s want %s", evidence.EventID, rule.RuleEventID)
	}
	return &ScoreResult{
		ScoreID: fixture.ScoreID, TenantID: fixture.TenantID, ProductID: fixture.ProductID,
		DatasetID: fixture.DatasetID, DatasetSplit: fixture.DatasetSplit,
		TraceID: fixture.TraceID, HandID: fixture.HandID, TableID: fixture.TableID,
		PlayedAt: fixture.PlayedAt, ScoredAt: fixture.DecidedAt,
		ModelName: "pair-catboost-v1", ModelRunID: "pair_test_run",
		FeatureDefinitionVersion: "pair-features-v1", DecisionPolicyVersion: 1,
		DecisionThreshold: 0.8, HandRiskProbability: 0.9,
		Alert:                fixture.ModelThresholdExceeded,
		RuleEvidenceEventIDs: []string{evidence.EventID},
		RuleEvidenceEvents:   []RuleEvidenceEvent{evidence},
	}
}

func TestReviewPolicyMatchesPythonGoldenAndReplay(t *testing.T) {
	fixture := loadReviewPolicyGolden(t)
	policy, err := LoadReviewPolicy("../../../../schemas/policies/review-policy-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	result := goldenPolicyScore(t, fixture)
	before := *result
	first, err := EvaluateReviewPolicy(result, fixture.RiskScoreEventID, policy)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := EvaluateReviewPolicy(result, fixture.RiskScoreEventID, policy)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, replay) || !reflect.DeepEqual(before, *result) {
		t.Fatal("review policy replay changed its output or model score input")
	}
	if first.EventID != fixture.Expected.DecisionID || first.Payload.Outcome != fixture.Expected.Outcome ||
		first.Payload.Action != fixture.Expected.Action ||
		!reflect.DeepEqual(first.Payload.ReasonCodes, fixture.Expected.ReasonCodes) ||
		first.Payload.RuleEvidence[0].Category != fixture.Expected.RuleCategory {
		t.Fatalf("review decision does not match golden fixture: %+v", first)
	}
}

func TestSoftRulesNeverRequireReviewAndFutureHardRuleDoes(t *testing.T) {
	fixture := loadReviewPolicyGolden(t)
	result := goldenPolicyScore(t, fixture)
	result.Alert = false
	policy := DefaultReviewPolicy()
	soft, err := EvaluateReviewPolicy(result, fixture.RiskScoreEventID, policy)
	if err != nil {
		t.Fatal(err)
	}
	if soft.Payload.Outcome != "no_review" || soft.Payload.Action != "none" ||
		soft.Payload.RuleEvidence[0].Category != "soft" {
		t.Fatalf("soft evidence changed the review outcome: %+v", soft.Payload)
	}

	repeated := policy.SoftRules[len(policy.SoftRules)-1]
	policy.SoftRules = policy.SoftRules[:len(policy.SoftRules)-1]
	policy.HardRules = []RulePolicySpec{repeated}
	hard, err := EvaluateReviewPolicy(result, fixture.RiskScoreEventID, policy)
	if err != nil {
		t.Fatal(err)
	}
	if hard.Payload.Outcome != "mandatory_review" || hard.Payload.Action != "analyst_review" ||
		len(hard.Payload.ReasonCodes) != 1 ||
		hard.Payload.ReasonCodes[0] != "hard-rule.pair.repeated-fold-to-partner-wins.v1" {
		t.Fatalf("hard evidence did not mandate review: %+v", hard.Payload)
	}
}

func TestReviewPolicyRejectsUnknownRule(t *testing.T) {
	fixture := loadReviewPolicyGolden(t)
	result := goldenPolicyScore(t, fixture)
	result.RuleEvidenceEvents[0].Payload.RuleID = "pair.unknown"
	if _, err := EvaluateReviewPolicy(result, fixture.RiskScoreEventID, DefaultReviewPolicy()); err == nil {
		t.Fatal("unknown rule must fail closed")
	}
}
