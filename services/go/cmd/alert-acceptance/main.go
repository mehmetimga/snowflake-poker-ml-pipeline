package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/risk"
)

type scoreExpectation struct {
	SchemaVersion                int      `json:"schema_version"`
	DatasetID                    string   `json:"dataset_id"`
	HandID                       string   `json:"hand_id"`
	ModelName                    string   `json:"model_name"`
	ModelRunID                   string   `json:"model_run_id"`
	DecisionThreshold            float64  `json:"decision_threshold"`
	HandRiskProbability          float64  `json:"hand_risk_probability"`
	ProbabilityTolerance         float64  `json:"probability_tolerance"`
	HighestRiskPair              string   `json:"highest_risk_pair"`
	ExpectedAlert                bool     `json:"expected_alert"`
	SelectedDemoAlert            bool     `json:"selected_demo_alert"`
	ScoreID                      string   `json:"score_id"`
	RiskScoreEventID             string   `json:"risk_score_event_id"`
	ReviewDecisionEventID        string   `json:"review_decision_event_id"`
	RiskAlertEventID             *string  `json:"risk_alert_event_id"`
	ReviewPolicyID               string   `json:"review_policy_id"`
	ReviewPolicyVersion          int      `json:"review_policy_version"`
	ExpectedPolicyOutcome        string   `json:"expected_policy_outcome"`
	ExpectedRuleEvidenceEventIDs []string `json:"expected_rule_evidence_event_ids"`
	ExpectedSinkRows             struct {
		RiskScores      int `json:"risk_scores"`
		ReviewDecisions int `json:"review_decisions"`
		RiskAlerts      int `json:"risk_alerts"`
		RuleEvidence    int `json:"rule_evidence"`
	} `json:"expected_sink_rows"`
}

type measuredHand struct {
	Result         *risk.ScoreResult        `json:"score_result"`
	ScoreEvent     risk.RiskScoreEvent      `json:"risk_score_event"`
	DecisionEvent  risk.ReviewDecisionEvent `json:"review_decision_event"`
	AlertEvent     *risk.RiskAlertEvent     `json:"risk_alert_event"`
	RuleEvidence   []risk.RuleEvidenceEvent `json:"rule_evidence_events"`
	HighestPairKey string                   `json:"highest_risk_pair"`
}

type runtimeReport struct {
	SchemaVersion        int    `json:"schema_version"`
	Status               string `json:"status"`
	Runtime              string `json:"runtime"`
	InferenceBackend     string `json:"inference_backend"`
	ModelName            string `json:"model_name"`
	ModelRunID           string `json:"model_run_id"`
	HandsScored          int    `json:"hands_scored"`
	PairFeaturesScored   int    `json:"pair_features_scored"`
	ModelAlerts          int    `json:"model_alerts"`
	SelectedDemoAlerts   int    `json:"selected_demo_alerts"`
	ExactIdentityMatches int    `json:"exact_identity_matches"`
	ProbabilityMatches   int    `json:"probability_matches"`
	EvidenceMatches      int    `json:"evidence_matches"`
	DurationMS           int64  `json:"duration_ms"`
	Output               string `json:"output"`
}

func main() {
	dataset := flag.String("dataset", "../../data/datasets/multitable-alert-acceptance-v1", "sealed D6 acceptance pack")
	modelDir := flag.String("model-dir", "../../models/pair-catboost-full-v2", "frozen model artifact directory")
	reviewPolicyPath := flag.String("review-policy", "../../schemas/policies/review-policy-v1.json", "governed review policy")
	ruleRolloutPath := flag.String("rule-rollout", "../../schemas/rules/rule-rollout-v1.json", "governed rule rollout")
	tritonURL := flag.String("triton-url", "http://127.0.0.1:18000", "actual Triton V2 HTTP endpoint")
	output := flag.String("output", "../../data/runs/multitable-alert-acceptance-local/go-results.jsonl", "measured output JSONL")
	timeout := flag.Duration("timeout", 30*time.Second, "Triton request and readiness timeout")
	buildVersion := flag.String("build-version", "acceptance-local", "scorer build identity recorded in measured events")
	flag.Parse()

	report, err := run(
		*dataset,
		*modelDir,
		*reviewPolicyPath,
		*ruleRolloutPath,
		*tritonURL,
		*output,
		*timeout,
		*buildVersion,
	)
	if err != nil {
		log.Fatal(err)
	}
	value, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(value))
}

func run(
	dataset,
	modelDir,
	reviewPolicyPath,
	ruleRolloutPath,
	tritonURL,
	output string,
	timeout time.Duration,
	buildVersion string,
) (runtimeReport, error) {
	started := time.Now()
	bundle, err := risk.LoadArtifactBundle(modelDir)
	if err != nil {
		return runtimeReport{}, fmt.Errorf("load model artifacts: %w", err)
	}
	backend, err := risk.NewTritonBackend(
		tritonURL,
		bundle.Contract.Batching.TritonModel,
		&http.Client{Timeout: timeout},
	)
	if err != nil {
		return runtimeReport{}, fmt.Errorf("configure Triton: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := backend.Ready(ctx); err != nil {
		return runtimeReport{}, fmt.Errorf("Triton readiness: %w", err)
	}
	scorer, err := risk.NewScorerWithBuildVersion(bundle, backend, nil, buildVersion)
	if err != nil {
		return runtimeReport{}, fmt.Errorf("configure Go scorer: %w", err)
	}
	rollout, err := risk.LoadRuleRollout(ruleRolloutPath)
	if err != nil {
		return runtimeReport{}, fmt.Errorf("load rule rollout: %w", err)
	}
	if err := scorer.SetRuleRollout(rollout); err != nil {
		return runtimeReport{}, fmt.Errorf("apply rule rollout: %w", err)
	}
	policy, err := risk.LoadReviewPolicy(reviewPolicyPath)
	if err != nil {
		return runtimeReport{}, err
	}

	features, err := readFeatures(filepath.Join(dataset, "expected", "pair_features.jsonl"))
	if err != nil {
		return runtimeReport{}, err
	}
	grouped := make(map[string][]risk.PairFeatureEvent)
	for _, feature := range features {
		grouped[feature.Payload.HandID] = append(grouped[feature.Payload.HandID], feature)
	}
	handIDs := make([]string, 0, len(grouped))
	for handID := range grouped {
		handIDs = append(handIDs, handID)
	}
	sort.Slice(handIDs, func(left, right int) bool {
		leftEvents, rightEvents := grouped[handIDs[left]], grouped[handIDs[right]]
		if leftEvents[0].Payload.PlayedAt != rightEvents[0].Payload.PlayedAt {
			return leftEvents[0].Payload.PlayedAt < rightEvents[0].Payload.PlayedAt
		}
		return handIDs[left] < handIDs[right]
	})

	// Score every hand before opening the private post-score oracle.
	measured := make(map[string]measuredHand, len(handIDs))
	for _, handID := range handIDs {
		scoreCtx, scoreCancel := context.WithTimeout(context.Background(), timeout)
		result, scoreErr := scorer.ScoreHand(scoreCtx, grouped[handID])
		scoreCancel()
		if scoreErr != nil {
			return runtimeReport{}, fmt.Errorf("score hand %s: %w", handID, scoreErr)
		}
		scoreEvent, decisionEvent, alertEvent, buildErr :=
			risk.BuildSeparatedOutputEvents(result, policy)
		if buildErr != nil {
			return runtimeReport{}, fmt.Errorf("build hand %s outputs: %w", handID, buildErr)
		}
		highest := result.PairScores[0]
		for _, pair := range result.PairScores[1:] {
			if pair.CalibratedProbability > highest.CalibratedProbability {
				highest = pair
			}
		}
		measured[handID] = measuredHand{
			Result:         result,
			ScoreEvent:     scoreEvent,
			DecisionEvent:  decisionEvent,
			AlertEvent:     alertEvent,
			RuleEvidence:   append([]risk.RuleEvidenceEvent(nil), result.RuleEvidenceEvents...),
			HighestPairKey: highest.PairKey,
		}
	}

	expectations, err := readExpectations(
		filepath.Join(dataset, "private_oracle", "score_expectations.jsonl"),
	)
	if err != nil {
		return runtimeReport{}, err
	}
	alerts, selected, identities, probabilities, evidence := 0, 0, 0, 0, 0
	for _, expected := range expectations {
		actual, ok := measured[expected.HandID]
		if !ok {
			return runtimeReport{}, fmt.Errorf("oracle hand was not scored: %s", expected.HandID)
		}
		if err := compareIdentity(actual, expected); err != nil {
			return runtimeReport{}, fmt.Errorf("hand %s identity: %w", expected.HandID, err)
		}
		identities++
		if math.Abs(actual.Result.HandRiskProbability-expected.HandRiskProbability) >
			expected.ProbabilityTolerance {
			return runtimeReport{}, fmt.Errorf(
				"hand %s probability mismatch: actual=%.12f expected=%.12f tolerance=%g",
				expected.HandID,
				actual.Result.HandRiskProbability,
				expected.HandRiskProbability,
				expected.ProbabilityTolerance,
			)
		}
		if actual.HighestPairKey != expected.HighestRiskPair ||
			actual.Result.Alert != expected.ExpectedAlert {
			return runtimeReport{}, fmt.Errorf("hand %s score decision mismatch", expected.HandID)
		}
		probabilities++
		actualEvidence := actual.Result.RuleEvidenceEventIDs
		if !equalStrings(actualEvidence, expected.ExpectedRuleEvidenceEventIDs) {
			return runtimeReport{}, fmt.Errorf("hand %s evidence identity mismatch", expected.HandID)
		}
		evidence++
		if actual.Result.Alert {
			alerts++
		}
		if expected.SelectedDemoAlert && actual.Result.Alert {
			selected++
		}
	}
	if len(measured) != len(expectations) {
		return runtimeReport{}, errors.New("scored hand count differs from private oracle")
	}
	if err := writeMeasured(output, handIDs, measured); err != nil {
		return runtimeReport{}, err
	}
	absoluteOutput, _ := filepath.Abs(output)
	return runtimeReport{
		SchemaVersion:        1,
		Status:               "passed",
		Runtime:              "go-risk-scorer",
		InferenceBackend:     "triton-v2-http",
		ModelName:            bundle.Contract.ModelName,
		ModelRunID:           bundle.Contract.RunID,
		HandsScored:          len(measured),
		PairFeaturesScored:   len(features),
		ModelAlerts:          alerts,
		SelectedDemoAlerts:   selected,
		ExactIdentityMatches: identities,
		ProbabilityMatches:   probabilities,
		EvidenceMatches:      evidence,
		DurationMS:           time.Since(started).Milliseconds(),
		Output:               absoluteOutput,
	}, nil
}

func compareIdentity(actual measuredHand, expected scoreExpectation) error {
	if actual.Result.ScoreID != expected.ScoreID ||
		actual.ScoreEvent.EventID != expected.RiskScoreEventID ||
		actual.DecisionEvent.EventID != expected.ReviewDecisionEventID {
		return errors.New("score, score-event, or review-decision ID differs")
	}
	if actual.Result.ModelName != expected.ModelName ||
		actual.Result.ModelRunID != expected.ModelRunID ||
		actual.Result.DecisionThreshold != expected.DecisionThreshold {
		return errors.New("model or decision-policy binding differs")
	}
	if actual.DecisionEvent.Payload.PolicyID != expected.ReviewPolicyID ||
		actual.DecisionEvent.Payload.PolicyVersion != expected.ReviewPolicyVersion ||
		actual.DecisionEvent.Payload.Outcome != expected.ExpectedPolicyOutcome {
		return errors.New("review-policy result differs")
	}
	if expected.ExpectedAlert {
		if actual.AlertEvent == nil || expected.RiskAlertEventID == nil ||
			actual.AlertEvent.EventID != *expected.RiskAlertEventID {
			return errors.New("expected alert identity differs")
		}
	} else if actual.AlertEvent != nil || expected.RiskAlertEventID != nil {
		return errors.New("unexpected alert")
	}
	if expected.ExpectedSinkRows.RiskScores != 1 ||
		expected.ExpectedSinkRows.ReviewDecisions != 1 ||
		expected.ExpectedSinkRows.RiskAlerts != boolInt(actual.AlertEvent != nil) ||
		expected.ExpectedSinkRows.RuleEvidence != len(actual.RuleEvidence) {
		return errors.New("expected sink row counts differ from measured outputs")
	}
	return nil
}

func readFeatures(path string) ([]risk.PairFeatureEvent, error) {
	var values []risk.PairFeatureEvent
	if err := readJSONLines(path, func(value []byte) error {
		var event risk.PairFeatureEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return err
		}
		values = append(values, event)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("read public pair features: %w", err)
	}
	return values, nil
}

func readExpectations(path string) ([]scoreExpectation, error) {
	var values []scoreExpectation
	if err := readJSONLines(path, func(value []byte) error {
		var expected scoreExpectation
		if err := json.Unmarshal(value, &expected); err != nil {
			return err
		}
		if expected.SchemaVersion != 1 || expected.ProbabilityTolerance <= 0 {
			return errors.New("invalid private score expectation")
		}
		values = append(values, expected)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("read private post-score oracle: %w", err)
	}
	return values, nil
}

func readJSONLines(path string, consume func([]byte) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		if err := consume(scanner.Bytes()); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func writeMeasured(path string, handIDs []string, measured map[string]measuredHand) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create measured output directory: %w", err)
	}
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create measured output: %w", err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, handID := range handIDs {
		if err := encoder.Encode(measured[handID]); err != nil {
			return fmt.Errorf("write measured hand %s: %w", handID, err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close measured output: %w", err)
	}
	return nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
