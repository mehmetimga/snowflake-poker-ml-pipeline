package risk

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	ReviewDecisionEventType = "poker.review-decision.recorded"
	ReviewDecisionsTopic    = "poker.review-decisions.v1"
)

type RulePolicySpec struct {
	RuleID      string `json:"rule_id"`
	RuleVersion int    `json:"rule_version"`
}

type ReviewPolicyDefinition struct {
	SchemaVersion  int    `json:"schema_version"`
	PolicyID       string `json:"policy_id"`
	PolicyVersion  int    `json:"policy_version"`
	PolicyOwner    string `json:"policy_owner"`
	EffectiveFrom  string `json:"effective_from"`
	Mode           string `json:"mode"`
	ModelThreshold struct {
		Enabled    bool   `json:"enabled"`
		ReasonCode string `json:"reason_code"`
		Outcome    string `json:"outcome"`
	} `json:"model_threshold"`
	SoftRules           []RulePolicySpec `json:"soft_rules"`
	HardRules           []RulePolicySpec `json:"hard_rules"`
	UnknownRuleBehavior string           `json:"unknown_rule_behavior"`
	DataQualityBehavior string           `json:"data_quality_behavior"`
	Actions             struct {
		NoReview          string `json:"no_review"`
		ReviewRecommended string `json:"review_recommended"`
		MandatoryReview   string `json:"mandatory_review"`
	} `json:"actions"`
	RolloutGates struct {
		MaximumReviewRate          float64 `json:"maximum_review_rate"`
		MaximumMandatoryReviewRate float64 `json:"maximum_mandatory_review_rate"`
		MinimumDecisions           int     `json:"minimum_decisions"`
	} `json:"rollout_gates"`
}

type PolicyRuleReference struct {
	RuleEventID string `json:"rule_event_id"`
	RuleID      string `json:"rule_id"`
	RuleVersion int    `json:"rule_version"`
	Category    string `json:"category"`
}

type ReviewDecisionPayload struct {
	DecisionID             string                `json:"decision_id"`
	RiskScoreEventID       string                `json:"risk_score_event_id"`
	ScoreID                string                `json:"score_id"`
	HandID                 string                `json:"hand_id"`
	TableID                string                `json:"table_id"`
	PlayedAt               string                `json:"played_at"`
	PolicyID               string                `json:"policy_id"`
	PolicyVersion          int                   `json:"policy_version"`
	PolicyOwner            string                `json:"policy_owner"`
	PolicyMode             string                `json:"policy_mode"`
	Outcome                string                `json:"outcome"`
	Action                 string                `json:"action"`
	ReasonCodes            []string              `json:"reason_codes"`
	ModelThresholdExceeded bool                  `json:"model_threshold_exceeded"`
	RuleEvidence           []PolicyRuleReference `json:"rule_evidence"`
	DecidedAt              string                `json:"decided_at"`
}

type ReviewDecisionEvent struct {
	EventID       string                `json:"event_id"`
	EventType     string                `json:"event_type"`
	SchemaVersion int                   `json:"schema_version"`
	TenantID      string                `json:"tenant_id"`
	ProductID     string                `json:"product_id"`
	DatasetID     string                `json:"dataset_id"`
	DatasetSplit  string                `json:"dataset_split"`
	OccurredAt    string                `json:"occurred_at"`
	EmittedAt     string                `json:"emitted_at"`
	TraceID       string                `json:"trace_id"`
	Payload       ReviewDecisionPayload `json:"payload"`
}

func LoadReviewPolicy(path string) (ReviewPolicyDefinition, error) {
	file, err := os.Open(path)
	if err != nil {
		return ReviewPolicyDefinition{}, fmt.Errorf("open review policy: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var policy ReviewPolicyDefinition
	if err := decoder.Decode(&policy); err != nil {
		return ReviewPolicyDefinition{}, fmt.Errorf("decode review policy: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return ReviewPolicyDefinition{}, fmt.Errorf("review policy contains trailing JSON")
	}
	if err := policy.Validate(); err != nil {
		return ReviewPolicyDefinition{}, err
	}
	return policy, nil
}

func DefaultReviewPolicy() ReviewPolicyDefinition {
	policy := ReviewPolicyDefinition{
		SchemaVersion: 1, PolicyID: "poker.review-routing", PolicyVersion: 1,
		PolicyOwner: "risk-operations", EffectiveFrom: "2026-07-21T00:00:00Z",
		Mode: "shadow", UnknownRuleBehavior: "reject", DataQualityBehavior: "dead_letter",
		SoftRules: []RulePolicySpec{
			{RuleID: "pair.one-folded-other-won", RuleVersion: 1},
			{RuleID: "pair.same-device", RuleVersion: 1},
			{RuleID: "pair.same-network", RuleVersion: 1},
			{RuleID: "pair.outcome-asymmetry", RuleVersion: 1},
			{RuleID: "pair.a-fold-b-win-rate", RuleVersion: 1},
			{RuleID: "pair.b-fold-a-win-rate", RuleVersion: 1},
			{RuleID: "pair.repeated-fold-to-partner-wins", RuleVersion: 1},
		},
		HardRules: []RulePolicySpec{},
	}
	policy.ModelThreshold.Enabled = true
	policy.ModelThreshold.ReasonCode = "model.threshold-exceeded"
	policy.ModelThreshold.Outcome = "review_recommended"
	policy.Actions.NoReview = "none"
	policy.Actions.ReviewRecommended = "analyst_review"
	policy.Actions.MandatoryReview = "analyst_review"
	policy.RolloutGates.MaximumReviewRate = 0.02
	policy.RolloutGates.MaximumMandatoryReviewRate = 0
	policy.RolloutGates.MinimumDecisions = 1000
	return policy
}

func (policy ReviewPolicyDefinition) Validate() error {
	if policy.SchemaVersion != 1 || policy.PolicyVersion < 1 ||
		!ruleIDPattern.MatchString(policy.PolicyID) || strings.TrimSpace(policy.PolicyOwner) == "" {
		return fmt.Errorf("review policy identity is invalid")
	}
	if _, err := time.Parse(time.RFC3339Nano, policy.EffectiveFrom); err != nil {
		return fmt.Errorf("review policy effective_from is invalid: %w", err)
	}
	if !containsRuleValue([]string{"shadow", "enforced"}, policy.Mode) ||
		!policy.ModelThreshold.Enabled || policy.ModelThreshold.ReasonCode != "model.threshold-exceeded" ||
		policy.ModelThreshold.Outcome != "review_recommended" ||
		policy.UnknownRuleBehavior != "reject" || policy.DataQualityBehavior != "dead_letter" {
		return fmt.Errorf("unsupported review policy semantics")
	}
	if policy.Actions.NoReview != "none" || policy.Actions.ReviewRecommended != "analyst_review" ||
		policy.Actions.MandatoryReview != "analyst_review" {
		return fmt.Errorf("unsupported review policy actions")
	}
	if policy.RolloutGates.MaximumReviewRate < 0 || policy.RolloutGates.MaximumReviewRate > 1 ||
		policy.RolloutGates.MaximumMandatoryReviewRate < 0 || policy.RolloutGates.MaximumMandatoryReviewRate > 1 ||
		policy.RolloutGates.MinimumDecisions < 1 {
		return fmt.Errorf("invalid review rollout gates")
	}
	seen := make(map[string]struct{})
	for _, category := range [][]RulePolicySpec{policy.SoftRules, policy.HardRules} {
		for _, rule := range category {
			key := fmt.Sprintf("%s:v%d", rule.RuleID, rule.RuleVersion)
			if !ruleIDPattern.MatchString(rule.RuleID) || rule.RuleVersion < 1 {
				return fmt.Errorf("invalid governed rule %s", key)
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("a rule version must have exactly one policy category")
			}
			seen[key] = struct{}{}
		}
	}
	return nil
}

func EvaluateReviewPolicy(
	result *ScoreResult,
	riskScoreEventID string,
	policy ReviewPolicyDefinition,
) (ReviewDecisionEvent, error) {
	if result == nil || !uuidPattern.MatchString(riskScoreEventID) {
		return ReviewDecisionEvent{}, fmt.Errorf("score and risk-score event identity are required")
	}
	if err := policy.Validate(); err != nil {
		return ReviewDecisionEvent{}, err
	}
	if len(result.RuleEvidenceEvents) != len(result.RuleEvidenceEventIDs) {
		return ReviewDecisionEvent{}, fmt.Errorf("policy evidence does not match score references")
	}
	categories := make(map[string]string)
	for _, rule := range policy.SoftRules {
		categories[fmt.Sprintf("%s:v%d", rule.RuleID, rule.RuleVersion)] = "soft"
	}
	for _, rule := range policy.HardRules {
		categories[fmt.Sprintf("%s:v%d", rule.RuleID, rule.RuleVersion)] = "hard"
	}
	references := make([]PolicyRuleReference, 0, len(result.RuleEvidenceEvents))
	for index, event := range result.RuleEvidenceEvents {
		if err := event.Validate(); err != nil {
			return ReviewDecisionEvent{}, fmt.Errorf("validate policy rule evidence: %w", err)
		}
		if event.EventID != result.RuleEvidenceEventIDs[index] || event.TenantID != result.TenantID ||
			event.ProductID != result.ProductID || event.DatasetID != result.DatasetID ||
			event.DatasetSplit != result.DatasetSplit || event.TraceID != result.TraceID ||
			event.Payload.HandID != result.HandID {
			return ReviewDecisionEvent{}, fmt.Errorf("rule evidence does not match policy score scope")
		}
		key := fmt.Sprintf("%s:v%d", event.Payload.RuleID, event.Payload.RuleVersion)
		category, ok := categories[key]
		if !ok {
			return ReviewDecisionEvent{}, fmt.Errorf("rule %s is not governed by %s:v%d", key, policy.PolicyID, policy.PolicyVersion)
		}
		references = append(references, PolicyRuleReference{
			RuleEventID: event.EventID, RuleID: event.Payload.RuleID,
			RuleVersion: event.Payload.RuleVersion, Category: category,
		})
	}
	sort.Slice(references, func(left, right int) bool {
		return references[left].RuleEventID < references[right].RuleEventID
	})
	hardReasons := make([]string, 0)
	for _, reference := range references {
		if reference.Category == "hard" {
			hardReasons = append(hardReasons, fmt.Sprintf(
				"hard-rule.%s.v%d", reference.RuleID, reference.RuleVersion))
		}
	}
	sort.Strings(hardReasons)
	outcome, action := "no_review", policy.Actions.NoReview
	if len(hardReasons) > 0 {
		outcome, action = "mandatory_review", policy.Actions.MandatoryReview
	} else if result.Alert {
		outcome, action = policy.ModelThreshold.Outcome, policy.Actions.ReviewRecommended
	}
	reasons := make([]string, 0, len(hardReasons)+1)
	if result.Alert {
		reasons = append(reasons, policy.ModelThreshold.ReasonCode)
	}
	reasons = append(reasons, hardReasons...)
	decisionID := stableReviewDecisionID(
		result.TenantID, result.ProductID, result.DatasetID, result.DatasetSplit,
		policy.PolicyID, policy.PolicyVersion, riskScoreEventID)
	event := ReviewDecisionEvent{
		EventID: decisionID, EventType: ReviewDecisionEventType, SchemaVersion: 1,
		TenantID: result.TenantID, ProductID: result.ProductID,
		DatasetID: result.DatasetID, DatasetSplit: result.DatasetSplit,
		OccurredAt: result.PlayedAt, EmittedAt: result.ScoredAt, TraceID: result.TraceID,
		Payload: ReviewDecisionPayload{
			DecisionID: decisionID, RiskScoreEventID: riskScoreEventID,
			ScoreID: result.ScoreID, HandID: result.HandID, TableID: result.TableID,
			PlayedAt: result.PlayedAt, PolicyID: policy.PolicyID,
			PolicyVersion: policy.PolicyVersion, PolicyOwner: policy.PolicyOwner,
			PolicyMode: policy.Mode, Outcome: outcome, Action: action,
			ReasonCodes: reasons, ModelThresholdExceeded: result.Alert,
			RuleEvidence: references, DecidedAt: result.ScoredAt,
		},
	}
	if err := event.Validate(); err != nil {
		return ReviewDecisionEvent{}, err
	}
	return event, nil
}

func (event ReviewDecisionEvent) Validate() error {
	if event.EventType != ReviewDecisionEventType || event.SchemaVersion != 1 ||
		event.EventID != event.Payload.DecisionID || event.TenantID == "" ||
		event.ProductID == "" || event.DatasetID == "" || event.DatasetSplit == "" ||
		!uuidPattern.MatchString(event.TraceID) {
		return fmt.Errorf("review-decision envelope identity is invalid")
	}
	payload := event.Payload
	if !uuidPattern.MatchString(payload.RiskScoreEventID) || payload.ScoreID == "" ||
		payload.HandID == "" || payload.TableID == "" || payload.PolicyOwner == "" ||
		payload.PolicyVersion < 1 || !ruleIDPattern.MatchString(payload.PolicyID) {
		return fmt.Errorf("review-decision payload identity is invalid")
	}
	if len(payload.ScoreID) < 32 || len(payload.ScoreID) > 64 ||
		!containsRuleValue([]string{"shadow", "enforced"}, payload.PolicyMode) ||
		len(payload.ReasonCodes) > 32 || len(payload.RuleEvidence) > 256 {
		return fmt.Errorf("review-decision payload bounds or policy mode are invalid")
	}
	if event.OccurredAt != payload.PlayedAt || event.EmittedAt != payload.DecidedAt {
		return fmt.Errorf("review-decision timestamps do not match its score")
	}
	playedAt, err := time.Parse(time.RFC3339Nano, payload.PlayedAt)
	if err != nil {
		return fmt.Errorf("review-decision played_at is invalid: %w", err)
	}
	decidedAt, err := time.Parse(time.RFC3339Nano, payload.DecidedAt)
	if err != nil {
		return fmt.Errorf("review-decision decided_at is invalid: %w", err)
	}
	if decidedAt.Before(playedAt) {
		return fmt.Errorf("review decision cannot precede hand time")
	}
	hardReasons := make(map[string]struct{})
	seenRules := make(map[string]struct{})
	for _, reference := range payload.RuleEvidence {
		if !uuidPattern.MatchString(reference.RuleEventID) || !ruleIDPattern.MatchString(reference.RuleID) ||
			reference.RuleVersion < 1 || !containsRuleValue([]string{"soft", "hard"}, reference.Category) {
			return fmt.Errorf("invalid review-decision rule reference")
		}
		if _, duplicate := seenRules[reference.RuleEventID]; duplicate {
			return fmt.Errorf("review-decision rule references must be unique")
		}
		seenRules[reference.RuleEventID] = struct{}{}
		if reference.Category == "hard" {
			hardReasons[fmt.Sprintf("hard-rule.%s.v%d", reference.RuleID, reference.RuleVersion)] = struct{}{}
		}
	}
	expectedOutcome := "no_review"
	if len(hardReasons) > 0 {
		expectedOutcome = "mandatory_review"
	} else if payload.ModelThresholdExceeded {
		expectedOutcome = "review_recommended"
	}
	expectedAction := "analyst_review"
	if expectedOutcome == "no_review" {
		expectedAction = "none"
	}
	if payload.Outcome != expectedOutcome || payload.Action != expectedAction {
		return fmt.Errorf("review-decision outcome or action does not match policy inputs")
	}
	seenReasons := make(map[string]struct{})
	actualHard := make(map[string]struct{})
	modelReason := false
	for _, reason := range payload.ReasonCodes {
		if strings.TrimSpace(reason) == "" {
			return fmt.Errorf("review-decision reason codes cannot be empty")
		}
		if _, duplicate := seenReasons[reason]; duplicate {
			return fmt.Errorf("review-decision reason codes must be unique")
		}
		seenReasons[reason] = struct{}{}
		modelReason = modelReason || reason == "model.threshold-exceeded"
		if strings.HasPrefix(reason, "hard-rule.") {
			actualHard[reason] = struct{}{}
		}
	}
	if modelReason != payload.ModelThresholdExceeded || !equalStringSets(actualHard, hardReasons) {
		return fmt.Errorf("review-decision reason codes are inconsistent")
	}
	expectedID := stableReviewDecisionID(
		event.TenantID, event.ProductID, event.DatasetID, event.DatasetSplit,
		payload.PolicyID, payload.PolicyVersion, payload.RiskScoreEventID)
	if event.EventID != expectedID {
		return fmt.Errorf("review decision ID is not the deterministic replay identity")
	}
	return nil
}

func stableReviewDecisionID(
	tenantID, productID, datasetID, datasetSplit, policyID string,
	policyVersion int,
	riskScoreEventID string,
) string {
	return uuidV5URL(strings.Join([]string{
		tenantID, productID, datasetID, datasetSplit, policyID,
		fmt.Sprint(policyVersion), riskScoreEventID,
	}, "\x1f"))
}

func equalStringSets(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if _, ok := right[value]; !ok {
			return false
		}
	}
	return true
}
