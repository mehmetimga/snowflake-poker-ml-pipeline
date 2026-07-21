package risk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

const (
	RiskScoreEventType = "poker.risk-score.computed"
	RiskAlertEventType = "poker.risk-alert.created"
)

type RiskScorePayload struct {
	ScoreID                  string        `json:"score_id"`
	HandID                   string        `json:"hand_id"`
	TableID                  string        `json:"table_id"`
	PlayedAt                 string        `json:"played_at"`
	ModelName                string        `json:"model_name"`
	ModelRunID               string        `json:"model_run_id"`
	FeatureDefinitionVersion string        `json:"feature_definition_version"`
	DecisionPolicyVersion    int           `json:"decision_policy_version"`
	DecisionThreshold        float64       `json:"decision_threshold"`
	ServiceImplementation    string        `json:"service_implementation"`
	ServiceBuildVersion      string        `json:"service_build_version"`
	ScoredAt                 string        `json:"scored_at"`
	RuleEvidenceEventIDs     []string      `json:"rule_evidence_event_ids"`
	PairScores               []PairScore   `json:"pair_scores"`
	PlayerScores             []PlayerScore `json:"player_scores"`
	HandRiskProbability      float64       `json:"hand_risk_probability"`
	Alert                    bool          `json:"alert"`
}

type RiskScoreEvent struct {
	EventID       string           `json:"event_id"`
	EventType     string           `json:"event_type"`
	SchemaVersion int              `json:"schema_version"`
	TenantID      string           `json:"tenant_id"`
	ProductID     string           `json:"product_id"`
	DatasetID     string           `json:"dataset_id"`
	DatasetSplit  string           `json:"dataset_split"`
	OccurredAt    string           `json:"occurred_at"`
	EmittedAt     string           `json:"emitted_at"`
	TraceID       string           `json:"trace_id"`
	Payload       RiskScorePayload `json:"payload"`
}

type RiskAlertPayload struct {
	AlertID                  string        `json:"alert_id"`
	RiskScoreEventID         string        `json:"risk_score_event_id"`
	ScoreID                  string        `json:"score_id"`
	HandID                   string        `json:"hand_id"`
	TableID                  string        `json:"table_id"`
	PlayedAt                 string        `json:"played_at"`
	ModelName                string        `json:"model_name"`
	ModelRunID               string        `json:"model_run_id"`
	FeatureDefinitionVersion string        `json:"feature_definition_version"`
	DecisionPolicyVersion    int           `json:"decision_policy_version"`
	DecisionThreshold        float64       `json:"decision_threshold"`
	ServiceImplementation    string        `json:"service_implementation"`
	ServiceBuildVersion      string        `json:"service_build_version"`
	ReviewDecisionEventID    string        `json:"review_decision_event_id"`
	ReviewPolicyID           string        `json:"review_policy_id"`
	ReviewPolicyVersion      int           `json:"review_policy_version"`
	ReviewPolicyMode         string        `json:"review_policy_mode"`
	PolicyOutcome            string        `json:"policy_outcome"`
	PolicyReasonCodes        []string      `json:"policy_reason_codes"`
	RuleEvidenceEventIDs     []string      `json:"rule_evidence_event_ids"`
	RiskProbability          float64       `json:"risk_probability"`
	HighestRiskPair          PairScore     `json:"highest_risk_pair"`
	HighestRiskPlayers       []PlayerScore `json:"highest_risk_players"`
	ScoredAt                 string        `json:"scored_at"`
}

type RiskAlertEvent struct {
	EventID       string           `json:"event_id"`
	EventType     string           `json:"event_type"`
	SchemaVersion int              `json:"schema_version"`
	TenantID      string           `json:"tenant_id"`
	ProductID     string           `json:"product_id"`
	DatasetID     string           `json:"dataset_id"`
	DatasetSplit  string           `json:"dataset_split"`
	OccurredAt    string           `json:"occurred_at"`
	EmittedAt     string           `json:"emitted_at"`
	TraceID       string           `json:"trace_id"`
	Payload       RiskAlertPayload `json:"payload"`
}

func BuildOutputEvents(result *ScoreResult) (RiskScoreEvent, *RiskAlertEvent, error) {
	score, _, alert, err := BuildSeparatedOutputEvents(result, DefaultReviewPolicy())
	return score, alert, err
}

func BuildSeparatedOutputEvents(
	result *ScoreResult,
	policy ReviewPolicyDefinition,
) (RiskScoreEvent, ReviewDecisionEvent, *RiskAlertEvent, error) {
	if result == nil || result.ScoreID == "" || len(result.PairScores) != 15 || len(result.PlayerScores) != 6 {
		return RiskScoreEvent{}, ReviewDecisionEvent{}, nil, fmt.Errorf("complete score result is required")
	}
	if result.DecisionPolicyVersion < 1 || result.ServiceImplementation == "" || result.ServiceBuildVersion == "" {
		return RiskScoreEvent{}, ReviewDecisionEvent{}, nil, fmt.Errorf("score audit versions are incomplete")
	}
	if err := validateRuleEvidenceReferences(result.RuleEvidenceEventIDs); err != nil {
		return RiskScoreEvent{}, ReviewDecisionEvent{}, nil, err
	}
	scoreEventID := stableUUID(RiskScoreEventType, result.ScoreID)
	ruleEvidenceEventIDs := append([]string{}, result.RuleEvidenceEventIDs...)
	scoreEvent := RiskScoreEvent{
		EventID: scoreEventID, EventType: RiskScoreEventType, SchemaVersion: 1,
		TenantID: result.TenantID, ProductID: result.ProductID,
		DatasetID: result.DatasetID, DatasetSplit: result.DatasetSplit,
		OccurredAt: result.PlayedAt, EmittedAt: result.ScoredAt, TraceID: result.TraceID,
		Payload: RiskScorePayload{
			ScoreID: result.ScoreID, HandID: result.HandID, TableID: result.TableID,
			PlayedAt: result.PlayedAt, ModelName: result.ModelName, ModelRunID: result.ModelRunID,
			FeatureDefinitionVersion: result.FeatureDefinitionVersion,
			DecisionPolicyVersion:    result.DecisionPolicyVersion,
			DecisionThreshold:        result.DecisionThreshold,
			ServiceImplementation:    result.ServiceImplementation,
			ServiceBuildVersion:      result.ServiceBuildVersion, ScoredAt: result.ScoredAt,
			RuleEvidenceEventIDs: ruleEvidenceEventIDs,
			PairScores:           result.PairScores, PlayerScores: result.PlayerScores,
			HandRiskProbability: result.HandRiskProbability, Alert: result.Alert,
		},
	}
	decisionEvent, err := EvaluateReviewPolicy(result, scoreEventID, policy)
	if err != nil {
		return RiskScoreEvent{}, ReviewDecisionEvent{}, nil, err
	}
	if decisionEvent.Payload.Outcome == "no_review" {
		return scoreEvent, decisionEvent, nil, nil
	}
	highest := result.PairScores[0]
	for _, pair := range result.PairScores[1:] {
		if pair.CalibratedProbability > highest.CalibratedProbability {
			highest = pair
		}
	}
	players := make([]PlayerScore, 0, 2)
	for _, player := range result.PlayerScores {
		if player.PlayerID == highest.PlayerA || player.PlayerID == highest.PlayerB {
			players = append(players, player)
		}
	}
	sort.Slice(players, func(left, right int) bool { return players[left].PlayerID < players[right].PlayerID })
	if len(players) != 2 {
		return RiskScoreEvent{}, ReviewDecisionEvent{}, nil, fmt.Errorf("highest-risk pair players are missing")
	}
	alertID := stableUUID(RiskAlertEventType, result.ScoreID, decisionEvent.EventID)
	alertEvent := &RiskAlertEvent{
		EventID: alertID, EventType: RiskAlertEventType, SchemaVersion: 1,
		TenantID: result.TenantID, ProductID: result.ProductID,
		DatasetID: result.DatasetID, DatasetSplit: result.DatasetSplit,
		OccurredAt: result.PlayedAt, EmittedAt: result.ScoredAt, TraceID: result.TraceID,
		Payload: RiskAlertPayload{
			AlertID: alertID, RiskScoreEventID: scoreEventID, ScoreID: result.ScoreID,
			HandID: result.HandID, TableID: result.TableID, PlayedAt: result.PlayedAt,
			ModelName: result.ModelName, ModelRunID: result.ModelRunID,
			FeatureDefinitionVersion: result.FeatureDefinitionVersion,
			DecisionPolicyVersion:    result.DecisionPolicyVersion,
			DecisionThreshold:        result.DecisionThreshold,
			ServiceImplementation:    result.ServiceImplementation,
			ServiceBuildVersion:      result.ServiceBuildVersion,
			ReviewDecisionEventID:    decisionEvent.EventID,
			ReviewPolicyID:           decisionEvent.Payload.PolicyID,
			ReviewPolicyVersion:      decisionEvent.Payload.PolicyVersion,
			ReviewPolicyMode:         decisionEvent.Payload.PolicyMode,
			PolicyOutcome:            decisionEvent.Payload.Outcome,
			PolicyReasonCodes:        append([]string{}, decisionEvent.Payload.ReasonCodes...),
			RuleEvidenceEventIDs:     append([]string{}, ruleEvidenceEventIDs...),
			RiskProbability:          result.HandRiskProbability,
			HighestRiskPair:          highest, HighestRiskPlayers: players, ScoredAt: result.ScoredAt,
		},
	}
	return scoreEvent, decisionEvent, alertEvent, nil
}

func stableUUID(parts ...string) string {
	digest := sha256.Sum256([]byte(fmt.Sprint(parts)))
	bytes := digest[:16]
	bytes[6] = (bytes[6] & 0x0f) | 0x50
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	hexValue := hex.EncodeToString(bytes)
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexValue[0:8], hexValue[8:12], hexValue[12:16], hexValue[16:20], hexValue[20:32])
}
