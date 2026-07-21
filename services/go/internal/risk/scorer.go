package risk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type InferenceBackend interface {
	Infer(ctx context.Context, inputName, outputName string, rows [][]float32) ([][]float32, error)
	Ready(ctx context.Context) error
}

type PairScore struct {
	EventID               string  `json:"feature_event_id"`
	PairKey               string  `json:"pair_key"`
	PlayerA               string  `json:"player_a"`
	PlayerB               string  `json:"player_b"`
	SnapshotRevision      int     `json:"snapshot_revision"`
	RawProbability        float64 `json:"raw_probability"`
	CalibratedProbability float64 `json:"calibrated_probability"`
	Alert                 bool    `json:"alert"`
}

type PlayerScore struct {
	PlayerID        string  `json:"player_id"`
	RiskProbability float64 `json:"risk_probability"`
	Alert           bool    `json:"alert"`
}

type ScoreResult struct {
	ScoreID                  string              `json:"score_id"`
	TenantID                 string              `json:"tenant_id"`
	ProductID                string              `json:"product_id"`
	DatasetID                string              `json:"dataset_id"`
	DatasetSplit             string              `json:"dataset_split"`
	TraceID                  string              `json:"trace_id"`
	HandID                   string              `json:"hand_id"`
	TableID                  string              `json:"table_id"`
	PlayedAt                 string              `json:"played_at"`
	ModelName                string              `json:"model_name"`
	ModelRunID               string              `json:"model_run_id"`
	FeatureDefinitionVersion string              `json:"feature_definition_version"`
	DecisionPolicyVersion    int                 `json:"decision_policy_version"`
	DecisionThreshold        float64             `json:"decision_threshold"`
	ServiceImplementation    string              `json:"service_implementation"`
	ServiceBuildVersion      string              `json:"service_build_version"`
	ScoredAt                 string              `json:"scored_at"`
	RuleEvidenceEventIDs     []string            `json:"rule_evidence_event_ids"`
	RuleEvidenceEvents       []RuleEvidenceEvent `json:"-"`
	PairScores               []PairScore         `json:"pair_scores"`
	PlayerScores             []PlayerScore       `json:"player_scores"`
	HandRiskProbability      float64             `json:"hand_risk_probability"`
	Alert                    bool                `json:"alert"`
}

type Scorer struct {
	bundle              *ArtifactBundle
	backend             InferenceBackend
	clock               func() time.Time
	serviceBuildVersion string
	pairRulesEnabled    bool
	pairRuleEnablement  map[string]bool
}

func NewScorer(bundle *ArtifactBundle, backend InferenceBackend, clock func() time.Time) (*Scorer, error) {
	return NewScorerWithBuildVersion(bundle, backend, clock, "dev")
}

func NewScorerWithBuildVersion(bundle *ArtifactBundle, backend InferenceBackend, clock func() time.Time, buildVersion string) (*Scorer, error) {
	if bundle == nil || backend == nil {
		return nil, fmt.Errorf("artifact bundle and inference backend are required")
	}
	if strings.TrimSpace(buildVersion) == "" {
		return nil, fmt.Errorf("service build version is required")
	}
	if err := bundle.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = time.Now
	}
	return &Scorer{
		bundle: bundle, backend: backend, clock: clock,
		serviceBuildVersion: buildVersion, pairRulesEnabled: true,
	}, nil
}

// SetRuleRollout applies evidence-only enablement. Model preprocessing,
// inference, calibration, thresholds, and score aggregation are unaffected.
func (scorer *Scorer) SetRuleRollout(config *RuleRolloutConfig) error {
	if config == nil {
		return fmt.Errorf("rule rollout configuration is required")
	}
	enabled, err := config.GoRuleEnablement()
	if err != nil {
		return err
	}
	scorer.pairRuleEnablement = enabled
	return nil
}

func (scorer *Scorer) Ready(ctx context.Context) error {
	return scorer.backend.Ready(ctx)
}

func (scorer *Scorer) Contract() ScoringContract {
	return scorer.bundle.Contract
}

func (scorer *Scorer) ScoreHand(ctx context.Context, events []PairFeatureEvent) (*ScoreResult, error) {
	expected := scorer.bundle.Contract.Batching.ExpectedPairsPerSixPlayerHand
	if len(events) != expected {
		return nil, fmt.Errorf("complete-hand scoring requires %d pairs; got %d", expected, len(events))
	}
	events = sortedEvents(events)
	first := events[0]
	identity := []string{first.TenantID, first.ProductID, first.DatasetID, first.DatasetSplit, first.Payload.HandID, first.Payload.TableID, first.Payload.PlayedAt}
	players := make(map[string]struct{})
	pairKeys := make(map[string]struct{})
	rows := make([][]float32, 0, expected)
	for _, event := range events {
		if err := event.Validate(scorer.bundle.Contract.FeatureDefinitionVersion); err != nil {
			return nil, err
		}
		actual := []string{event.TenantID, event.ProductID, event.DatasetID, event.DatasetSplit, event.Payload.HandID, event.Payload.TableID, event.Payload.PlayedAt}
		if !equalStrings(identity, actual) {
			return nil, fmt.Errorf("pair rows do not belong to the same hand identity")
		}
		if _, duplicate := pairKeys[event.Payload.PairKey]; duplicate {
			return nil, fmt.Errorf("duplicate pair key %s", event.Payload.PairKey)
		}
		pairKeys[event.Payload.PairKey] = struct{}{}
		players[event.Payload.PlayerA] = struct{}{}
		players[event.Payload.PlayerB] = struct{}{}
		flat, err := event.Flatten(scorer.bundle.Contract.FeatureDefinitionVersion)
		if err != nil {
			return nil, err
		}
		row, err := scorer.bundle.Preprocessor.Transform(flat)
		if err != nil {
			return nil, fmt.Errorf("preprocess pair %s: %w", event.Payload.PairKey, err)
		}
		rows = append(rows, row)
	}
	if err := validateCompletePairSet(players, pairKeys); err != nil {
		return nil, err
	}

	scoredAt := scorer.clock().UTC()
	ruleEvidenceEvents := make([]RuleEvidenceEvent, 0)
	ruleEvidenceIDs := make(map[string]struct{})
	appendEvidence := func(event RuleEvidenceEvent) error {
		if _, duplicate := ruleEvidenceIDs[event.EventID]; duplicate {
			return fmt.Errorf("duplicate rule evidence ID %s across pair snapshots", event.EventID)
		}
		ruleEvidenceIDs[event.EventID] = struct{}{}
		ruleEvidenceEvents = append(ruleEvidenceEvents, event)
		return nil
	}
	for _, event := range events {
		for _, upstream := range event.UpstreamRuleEvidence {
			if err := appendEvidence(upstream); err != nil {
				return nil, err
			}
		}
		if scorer.pairRulesEnabled {
			fired, err := EvaluatePairRulesWithEnabled(event, scoredAt, scorer.pairRuleEnablement)
			if err != nil {
				return nil, fmt.Errorf("evaluate pair rules for %s: %w", event.Payload.PairKey, err)
			}
			for _, evidence := range fired {
				if err := appendEvidence(evidence); err != nil {
					return nil, err
				}
			}
		}
	}

	outputs, err := scorer.backend.Infer(ctx, scorer.bundle.Contract.Input.Name, scorer.bundle.Contract.Output.Name, rows)
	if err != nil {
		return nil, fmt.Errorf("model inference: %w", err)
	}
	if len(outputs) != len(events) {
		return nil, fmt.Errorf("inference returned %d rows; expected %d", len(outputs), len(events))
	}

	pairScores := make([]PairScore, 0, expected)
	playerRisk := make(map[string]float64)
	handRisk := 0.0
	threshold := scorer.bundle.Policy.Threshold
	positiveIndex := scorer.bundle.Contract.Output.PositiveClassIndex
	for index, output := range outputs {
		if len(output) != 2 {
			return nil, fmt.Errorf("inference row %d has %d outputs; expected 2", index, len(output))
		}
		for classIndex, probability := range output {
			if math.IsNaN(float64(probability)) || math.IsInf(float64(probability), 0) || probability < 0 || probability > 1 {
				return nil, fmt.Errorf("inference row %d class %d has an invalid probability", index, classIndex)
			}
		}
		if math.Abs(float64(output[0]+output[1])-1) > 1e-3 {
			return nil, fmt.Errorf("inference row %d probabilities do not sum to one", index)
		}
		raw := float64(output[positiveIndex])
		calibrated := calibrate(raw, scorer.bundle.Calibration)
		event := events[index]
		alert := calibrated >= threshold
		pairScores = append(pairScores, PairScore{
			EventID: event.EventID, PairKey: event.Payload.PairKey,
			PlayerA: event.Payload.PlayerA, PlayerB: event.Payload.PlayerB,
			SnapshotRevision: event.Payload.SnapshotRevision,
			RawProbability:   raw, CalibratedProbability: calibrated, Alert: alert,
		})
		for _, playerID := range []string{event.Payload.PlayerA, event.Payload.PlayerB} {
			if calibrated > playerRisk[playerID] {
				playerRisk[playerID] = calibrated
			}
		}
		if calibrated > handRisk {
			handRisk = calibrated
		}
	}
	playerIDs := make([]string, 0, len(playerRisk))
	for playerID := range playerRisk {
		playerIDs = append(playerIDs, playerID)
	}
	sort.Strings(playerIDs)
	playerScores := make([]PlayerScore, 0, len(playerIDs))
	for _, playerID := range playerIDs {
		playerScores = append(playerScores, PlayerScore{
			PlayerID: playerID, RiskProbability: playerRisk[playerID], Alert: playerRisk[playerID] >= threshold,
		})
	}

	ruleEvidenceEventIDs := make([]string, 0, len(ruleEvidenceEvents))
	for _, event := range ruleEvidenceEvents {
		ruleEvidenceEventIDs = append(ruleEvidenceEventIDs, event.EventID)
	}

	return &ScoreResult{
		ScoreID:  scoreIdentity(scorer.bundle.Contract.RunID, events),
		TenantID: first.TenantID, ProductID: first.ProductID,
		DatasetID: first.DatasetID, DatasetSplit: first.DatasetSplit, TraceID: first.TraceID,
		HandID: first.Payload.HandID, TableID: first.Payload.TableID, PlayedAt: first.Payload.PlayedAt,
		ModelName: scorer.bundle.Contract.ModelName, ModelRunID: scorer.bundle.Contract.RunID,
		FeatureDefinitionVersion: scorer.bundle.Contract.FeatureDefinitionVersion,
		DecisionPolicyVersion:    scorer.bundle.Policy.PolicyVersion,
		DecisionThreshold:        threshold, ServiceImplementation: "go-risk-scorer",
		ServiceBuildVersion:  scorer.serviceBuildVersion,
		ScoredAt:             scoredAt.Format(time.RFC3339Nano),
		RuleEvidenceEventIDs: ruleEvidenceEventIDs,
		RuleEvidenceEvents:   ruleEvidenceEvents,
		PairScores:           pairScores, PlayerScores: playerScores,
		HandRiskProbability: handRisk, Alert: handRisk >= threshold,
	}, nil
}

func validateCompletePairSet(players, pairs map[string]struct{}) error {
	if len(players) != 6 || len(pairs) != 15 {
		return fmt.Errorf("expected six players and 15 unique pairs; got %d players and %d pairs", len(players), len(pairs))
	}
	playerIDs := make([]string, 0, len(players))
	for playerID := range players {
		playerIDs = append(playerIDs, playerID)
	}
	sort.Strings(playerIDs)
	for left := 0; left < len(playerIDs); left++ {
		for right := left + 1; right < len(playerIDs); right++ {
			key := playerIDs[left] + ":" + playerIDs[right]
			if _, ok := pairs[key]; !ok {
				return fmt.Errorf("complete hand is missing pair %s", key)
			}
		}
	}
	return nil
}

func calibrate(probability float64, contract CalibrationContract) float64 {
	clipped := math.Max(1e-6, math.Min(1-1e-6, probability))
	logit := math.Log(clipped / (1 - clipped))
	value := contract.Slope*logit + contract.Intercept
	if value >= 0 {
		return 1 / (1 + math.Exp(-value))
	}
	exp := math.Exp(value)
	return exp / (1 + exp)
}

func scoreIdentity(runID string, events []PairFeatureEvent) string {
	parts := make([]string, 0, len(events)+1)
	parts = append(parts, runID)
	for _, event := range events {
		parts = append(parts, fmt.Sprintf("%s:%d:%s", event.Payload.PairKey, event.Payload.SnapshotRevision, event.EventID))
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(digest[:16])
}
