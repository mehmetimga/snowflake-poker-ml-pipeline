package risk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeBackend struct {
	readyErr error
	calls    int
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func (backend *fakeBackend) Infer(_ context.Context, inputName, outputName string, rows [][]float32) ([][]float32, error) {
	if inputName != "features" || outputName != "pair_probabilities" {
		return nil, fmt.Errorf("unexpected contract names")
	}
	backend.calls++
	result := make([][]float32, len(rows))
	for index, row := range rows {
		positive := float32(0.1)
		if row[0] > 0.5 {
			positive = 0.9
		}
		result[index] = []float32{1 - positive, positive}
	}
	return result, nil
}

func (backend *fakeBackend) Ready(_ context.Context) error { return backend.readyErr }

func intPointer(value int) *int { return &value }

func testBundle(t testing.TB) *ArtifactBundle {
	t.Helper()
	bundle := &ArtifactBundle{}
	bundle.Contract.ContractVersion = 1
	bundle.Contract.ModelName = "pair-catboost-v1"
	bundle.Contract.RunID = "pair_test_run"
	bundle.Contract.FeatureDefinitionVersion = "pair-features-v1"
	bundle.Contract.Input.Name = "features"
	bundle.Contract.Input.DType = "float32"
	bundle.Contract.Input.Shape = []*int{nil, intPointer(5)}
	bundle.Contract.Input.Preprocessing = "preprocessing.json"
	bundle.Contract.Input.OrderedFeatures = []string{
		"current_signal",
		"context_status_a==matched", "context_status_a==__UNKNOWN__",
		"context_status_b==matched", "context_status_b==__UNKNOWN__",
	}
	bundle.Contract.Output.Name = "pair_probabilities"
	bundle.Contract.Output.DType = "float32"
	bundle.Contract.Output.Shape = []*int{nil, intPointer(2)}
	bundle.Contract.Output.PositiveClassIndex = 1
	bundle.Contract.Calibration = "calibration.json"
	bundle.Contract.DecisionPolicy = "decision_policy.json"
	bundle.Contract.Batching.Unit = "hand"
	bundle.Contract.Batching.ExpectedPairsPerSixPlayerHand = 15
	bundle.Contract.Batching.TritonModel = "pair_catboost"
	bundle.Preprocessor = PreprocessingContract{
		ContractVersion:    1,
		NumericColumns:     []string{"current_signal"},
		CategoricalColumns: []string{"context_status_a", "context_status_b"},
		NumericFillValues:  map[string]float64{"current_signal": 0.25},
		CategoricalValues: map[string][]string{
			"context_status_a": {"matched", unknownCategory},
			"context_status_b": {"matched", unknownCategory},
		},
		OutputColumns: bundle.Contract.Input.OrderedFeatures,
		OutputDType:   "float32",
	}
	bundle.Calibration = CalibrationContract{Slope: 1, Intercept: 0, Method: "identity"}
	bundle.Policy.PolicyVersion = 1
	bundle.Policy.Threshold = 0.8
	bundle.Policy.PairsPerSixPlayerHand = 15
	bundle.Policy.Aggregation.Player = "max_pair_probability"
	bundle.Policy.Aggregation.Hand = "max_pair_probability"
	if err := bundle.Validate(); err != nil {
		t.Fatalf("test bundle is invalid: %v", err)
	}
	return bundle
}

func testEvents() []PairFeatureEvent {
	players := []string{"a", "b", "c", "d", "e", "f"}
	events := make([]PairFeatureEvent, 0, 15)
	for left := 0; left < len(players); left++ {
		for right := left + 1; right < len(players); right++ {
			pairKey := players[left] + ":" + players[right]
			signal := 0.0
			if pairKey == "a:b" {
				signal = 1
			}
			events = append(events, PairFeatureEvent{
				EventID: "event-" + pairKey, EventType: pairFeatureEventType, SchemaVersion: 1,
				TenantID: "tenant", ProductID: "poker", DatasetID: "dataset", DatasetSplit: "test",
				OccurredAt: "2026-07-20T00:00:00Z", EmittedAt: "2026-07-20T00:00:01Z", TraceID: "00000000-0000-5000-8000-000000000099",
				Payload: PairFeaturePayload{
					HandID: "hand-1", TableID: "table-1", PlayedAt: "2026-07-20T00:00:00Z",
					PairKey: pairKey, PlayerA: players[left], PlayerB: players[right], NumPlayers: 6,
					SourceHandEventID: "source-hand", SourcePlayerContextEventIDA: "context-a", SourcePlayerContextEventIDB: "context-b",
					SourceRevisionA: 1, SourceRevisionB: 1,
					SnapshotRevision: 1, FeatureDefinitionVersion: "pair-features-v1",
					ContextStatusA: "matched", ContextStatusB: "matched", ContextVersionA: intPointer(1), ContextVersionB: intPointer(1),
					CurrentHand: map[string]any{
						"signal": signal, "one_folded_other_won": false,
					},
					Context:      map[string]any{"same_device": false, "same_network": false},
					UserHistoryA: map[string]any{}, UserHistoryB: map[string]any{},
					PairHistory: map[string]any{
						"outcome_asymmetry": 0.0, "a_fold_b_win_rate": 0.0, "b_fold_a_win_rate": 0.0,
					},
				},
			})
		}
	}
	return events
}

func TestPreprocessorUsesFillValuesAndUnknownCategory(t *testing.T) {
	contract := testBundle(t).Preprocessor
	row, err := contract.Transform(map[string]any{
		"current_signal": nil, "context_status_a": "late", "context_status_b": "matched",
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := []float32{0.25, 0, 1, 1, 0}
	if fmt.Sprint(row) != fmt.Sprint(expected) {
		t.Fatalf("unexpected row: got %v want %v", row, expected)
	}
}

func TestPairFeatureRejectsPrivateTruth(t *testing.T) {
	event := testEvents()[0]
	event.Payload.Context["target"] = 1
	if _, err := event.Flatten("pair-features-v1"); err == nil || !strings.Contains(err.Error(), "private") {
		t.Fatalf("expected private-field error, got %v", err)
	}
}

func TestPairFeatureRejectsUnboundedUpstreamRuleEvidence(t *testing.T) {
	event := testEvents()[0]
	event.UpstreamRuleEvidence = make([]RuleEvidenceEvent, 33)
	if err := event.Validate("pair-features-v1"); err == nil ||
		!strings.Contains(err.Error(), "at most 32") {
		t.Fatalf("expected bounded upstream rule-evidence error, got %v", err)
	}
}

func TestAssemblerEmitsCompleteHandAndCorrection(t *testing.T) {
	assembler, err := NewHandAssembler(15, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	events := testEvents()
	for index, event := range events {
		batch, complete, err := assembler.Add(event, "pair-features-v1", now)
		if err != nil {
			t.Fatal(err)
		}
		if index < 14 && (complete || batch != nil) {
			t.Fatalf("hand completed too early at pair %d", index)
		}
		if index == 14 && (!complete || len(batch) != 15) {
			t.Fatalf("expected complete hand, got complete=%v rows=%d", complete, len(batch))
		}
	}
	if _, complete, err := assembler.Add(events[0], "pair-features-v1", now); err != nil || complete {
		t.Fatalf("duplicate should be ignored: complete=%v err=%v", complete, err)
	}
	corrected := events[0]
	corrected.EventID = "corrected-event"
	corrected.Payload.SnapshotRevision = 2
	batch, complete, err := assembler.Add(corrected, "pair-features-v1", now)
	if err != nil || !complete || len(batch) != 15 {
		t.Fatalf("correction should rescore complete hand: complete=%v rows=%d err=%v", complete, len(batch), err)
	}
}

func TestAssemblerNeverCombinesTenantsWithTheSameHandID(t *testing.T) {
	assembler, err := NewHandAssembler(15, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	events := testEvents()
	for index := 0; index < 14; index++ {
		if _, complete, err := assembler.Add(events[index], "pair-features-v1", now); err != nil || complete {
			t.Fatalf("tenant A partial hand should remain incomplete: %v", err)
		}
	}
	otherTenant := events[14]
	otherTenant.TenantID = "tenant-b"
	if _, complete, err := assembler.Add(otherTenant, "pair-features-v1", now); err != nil || complete {
		t.Fatalf("tenant B row must not complete tenant A hand: complete=%v err=%v", complete, err)
	}
	if assembler.Len() != 2 {
		t.Fatalf("expected two tenant-isolated buckets, got %d", assembler.Len())
	}
}

func TestScorerCalibratesAndAggregatesCompleteHand(t *testing.T) {
	backend := &fakeBackend{}
	clock := func() time.Time { return time.Date(2026, 7, 20, 1, 2, 3, 0, time.UTC) }
	scorer, err := NewScorer(testBundle(t), backend, clock)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scorer.ScoreHand(context.Background(), testEvents())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Alert || result.HandRiskProbability < 0.89 || len(result.PairScores) != 15 || len(result.PlayerScores) != 6 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.PairScores[0].PairKey != "a:b" || !result.PairScores[0].Alert {
		t.Fatalf("expected canonical a:b alert, got %+v", result.PairScores[0])
	}
	if backend.calls != 1 || result.ScoredAt != "2026-07-20T01:02:03Z" {
		t.Fatalf("unexpected backend calls or score time")
	}
}

func TestEnablingPairRulesDoesNotChangeModelProbabilityOrDecision(t *testing.T) {
	events := testEvents()
	events[0].Payload.CurrentHand["one_folded_other_won"] = true
	events[0].Payload.Context["same_device"] = true
	events[0].Payload.Context["same_network"] = true
	events[0].Payload.PairHistory["outcome_asymmetry"] = 0.4
	events[0].Payload.PairHistory["a_fold_b_win_rate"] = 0.25
	events[0].Payload.PairHistory["b_fold_a_win_rate"] = 0.75
	clock := func() time.Time { return time.Date(2026, 7, 20, 1, 2, 3, 0, time.UTC) }

	enabled, err := NewScorer(testBundle(t), &fakeBackend{}, clock)
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := NewScorer(testBundle(t), &fakeBackend{}, clock)
	if err != nil {
		t.Fatal(err)
	}
	disabled.pairRulesEnabled = false

	withRules, err := enabled.ScoreHand(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	withoutRules, err := disabled.ScoreHand(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(withRules.PairScores, withoutRules.PairScores) ||
		!reflect.DeepEqual(withRules.PlayerScores, withoutRules.PlayerScores) ||
		withRules.HandRiskProbability != withoutRules.HandRiskProbability ||
		withRules.Alert != withoutRules.Alert {
		t.Fatal("rule evaluation changed model probability or threshold decision")
	}
	if len(withRules.RuleEvidenceEvents) != 6 || len(withRules.RuleEvidenceEventIDs) != 6 ||
		len(withoutRules.RuleEvidenceEvents) != 0 || len(withoutRules.RuleEvidenceEventIDs) != 0 {
		t.Fatalf("unexpected rule evidence counts enabled=%d disabled=%d", len(withRules.RuleEvidenceEvents), len(withoutRules.RuleEvidenceEvents))
	}
}

func TestScorerCarriesValidatedUpstreamFlinkRuleEvidence(t *testing.T) {
	events := testEvents()
	evidence, err := BuildRuleEvidenceEvent(RuleEvidenceInput{
		TenantID: events[0].TenantID, ProductID: events[0].ProductID,
		DatasetID: events[0].DatasetID, DatasetSplit: events[0].DatasetSplit,
		TraceID: events[0].TraceID,
		RuleID:  "pair.repeated-fold-to-partner-wins", RuleVersion: 1,
		RuleOwner: "risk-analytics", EntityType: "pair",
		EntityKey: events[0].Payload.PairKey, HandID: events[0].Payload.HandID,
		ObservationRevision: events[0].Payload.SnapshotRevision,
		Severity:            "high", RawScore: 60,
		Evidence: map[string]any{
			"window_hand_count": 5, "directional_fold_win_rate": 0.6,
		},
		EffectiveAt:              events[0].Payload.PlayedAt,
		EmittedAt:                "2026-07-20T00:00:01Z",
		FeatureDefinitionVersion: events[0].Payload.FeatureDefinitionVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	events[0].UpstreamRuleEvidence = []RuleEvidenceEvent{evidence}
	scorer, err := NewScorer(testBundle(t), &fakeBackend{}, func() time.Time {
		return time.Date(2026, 7, 20, 1, 2, 3, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	scorer.pairRulesEnabled = false
	result, err := scorer.ScoreHand(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RuleEvidenceEvents) != 1 || result.RuleEvidenceEvents[0].EventID != evidence.EventID ||
		len(result.RuleEvidenceEventIDs) != 1 || result.RuleEvidenceEventIDs[0] != evidence.EventID {
		t.Fatal("upstream Flink evidence was not retained when local pair rules were disabled")
	}

	events[0].UpstreamRuleEvidence[0].Payload.ObservationRevision = 2
	if _, err := scorer.ScoreHand(context.Background(), events); err == nil ||
		!strings.Contains(err.Error(), "upstream rule evidence") {
		t.Fatalf("expected mismatched upstream evidence rejection, got %v", err)
	}
}

func TestOutputEventsUseDeterministicIDsAndAlertReference(t *testing.T) {
	scorer, err := NewScorer(testBundle(t), &fakeBackend{}, func() time.Time {
		return time.Date(2026, 7, 20, 1, 2, 3, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := scorer.ScoreHand(context.Background(), testEvents())
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := BuildRuleEvidenceEvent(RuleEvidenceInput{
		TenantID: result.TenantID, ProductID: result.ProductID,
		DatasetID: result.DatasetID, DatasetSplit: result.DatasetSplit,
		TraceID: result.TraceID, RuleID: "pair.same-device", RuleVersion: 1,
		RuleOwner: "trust-platform", EntityType: "pair", EntityKey: "a:b",
		HandID: result.HandID, ObservationRevision: 1, Severity: "high", RawScore: 100,
		Evidence:    map[string]any{"feature_name": "same_device", "observed_value": 1.0},
		EffectiveAt: result.PlayedAt, EmittedAt: result.ScoredAt,
		FeatureDefinitionVersion: result.FeatureDefinitionVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	result.RuleEvidenceEventIDs = []string{evidence.EventID}
	result.RuleEvidenceEvents = []RuleEvidenceEvent{evidence}
	firstScore, firstAlert, err := BuildOutputEvents(result)
	if err != nil {
		t.Fatal(err)
	}
	secondScore, secondAlert, err := BuildOutputEvents(result)
	if err != nil {
		t.Fatal(err)
	}
	if firstAlert == nil || secondAlert == nil {
		t.Fatal("alerting score must produce an alert event")
	}
	if firstScore.EventID != secondScore.EventID || firstAlert.EventID != secondAlert.EventID {
		t.Fatal("replay-stable score must produce deterministic event IDs")
	}
	if firstAlert.Payload.RiskScoreEventID != firstScore.EventID || firstAlert.Payload.HighestRiskPair.PairKey != "a:b" {
		t.Fatalf("alert does not reference the score/highest pair: %+v", firstAlert.Payload)
	}
	if len(firstScore.Payload.RuleEvidenceEventIDs) != 1 ||
		firstScore.Payload.RuleEvidenceEventIDs[0] != firstAlert.Payload.RuleEvidenceEventIDs[0] {
		t.Fatal("score and alert must reference the same rule evidence")
	}
}

func TestOutputEventsRejectDuplicateRuleEvidenceReferences(t *testing.T) {
	scorer, err := NewScorer(testBundle(t), &fakeBackend{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scorer.ScoreHand(context.Background(), testEvents())
	if err != nil {
		t.Fatal(err)
	}
	reference := "8bcfb4e4-2113-52c3-85c2-a6ca4cb19823"
	result.RuleEvidenceEventIDs = []string{reference, reference}
	if _, _, err := BuildOutputEvents(result); err == nil || !strings.Contains(err.Error(), "references must be unique") {
		t.Fatalf("expected duplicate rule reference rejection, got %v", err)
	}
}

func TestHardPolicyCanRouteReviewBelowThresholdWithoutChangingProbability(t *testing.T) {
	bundle := testBundle(t)
	bundle.Policy.Threshold = 0.95
	scorer, err := NewScorer(bundle, &fakeBackend{}, func() time.Time {
		return time.Date(2026, 7, 20, 1, 2, 3, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := scorer.ScoreHand(context.Background(), testEvents())
	if err != nil {
		t.Fatal(err)
	}
	if result.Alert || result.HandRiskProbability >= result.DecisionThreshold {
		t.Fatal("test score must remain below its model threshold")
	}
	probability := result.HandRiskProbability
	evidence, err := BuildRuleEvidenceEvent(RuleEvidenceInput{
		TenantID: result.TenantID, ProductID: result.ProductID,
		DatasetID: result.DatasetID, DatasetSplit: result.DatasetSplit,
		TraceID: result.TraceID, RuleID: "pair.repeated-fold-to-partner-wins", RuleVersion: 1,
		RuleOwner: "risk-analytics", EntityType: "pair", EntityKey: "a:b",
		HandID: result.HandID, ObservationRevision: 1, Severity: "high", RawScore: 60,
		Evidence:    map[string]any{"window_hand_count": 5, "directional_fold_win_rate": 0.6},
		EffectiveAt: result.PlayedAt, EmittedAt: result.ScoredAt,
		FeatureDefinitionVersion: result.FeatureDefinitionVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	result.RuleEvidenceEventIDs = []string{evidence.EventID}
	result.RuleEvidenceEvents = []RuleEvidenceEvent{evidence}
	policy := DefaultReviewPolicy()
	repeated := policy.SoftRules[len(policy.SoftRules)-1]
	policy.SoftRules = policy.SoftRules[:len(policy.SoftRules)-1]
	policy.HardRules = []RulePolicySpec{repeated}
	score, decision, alert, err := BuildSeparatedOutputEvents(result, policy)
	if err != nil {
		t.Fatal(err)
	}
	if score.Payload.Alert || decision.Payload.Outcome != "mandatory_review" || alert == nil ||
		alert.Payload.PolicyOutcome != "mandatory_review" ||
		alert.Payload.RiskProbability >= alert.Payload.DecisionThreshold ||
		result.HandRiskProbability != probability {
		t.Fatalf("hard review routing changed or contradicted model output: score=%+v decision=%+v alert=%+v", score.Payload, decision.Payload, alert)
	}
}

func TestTritonBackendUsesOneBatchedV2Request(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/v2/models/pair_catboost/ready":
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("{}"))}, nil
		case "/v2/models/pair_catboost/infer":
			var payload tritonRequest
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if len(payload.Inputs) != 1 || fmt.Sprint(payload.Inputs[0].Shape) != "[15 5]" || len(payload.Inputs[0].Data) != 75 {
				t.Fatalf("unexpected Triton request: %+v", payload)
			}
			data := make([]float32, 30)
			for index := 0; index < 15; index++ {
				data[index*2], data[index*2+1] = 0.8, 0.2
			}
			body, err := json.Marshal(tritonResponse{Outputs: []tritonOutput{{
				Name: "pair_probabilities", Shape: []int{15, 2}, Datatype: "FP32", Data: data,
			}}})
			if err != nil {
				t.Fatal(err)
			}
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(bytes.NewReader(body))}, nil
		default:
			return &http.Response{StatusCode: http.StatusNotFound, Status: "404 Not Found", Body: io.NopCloser(strings.NewReader(`{"error":"not found"}`))}, nil
		}
	})}
	backend, err := NewTritonBackend("http://triton.test", "pair_catboost", client)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows := make([][]float32, 15)
	for index := range rows {
		rows[index] = make([]float32, 5)
	}
	outputs, err := backend.Infer(context.Background(), "features", "pair_probabilities", rows)
	if err != nil || len(outputs) != 15 || outputs[0][1] != 0.2 {
		t.Fatalf("unexpected Triton output: rows=%v err=%v", len(outputs), err)
	}
}

func TestArtifactBundleVerifiesHashes(t *testing.T) {
	bundle := testBundle(t)
	root := t.TempDir()
	write := func(name string, value any) {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("scoring_contract.json", bundle.Contract)
	write("preprocessing.json", bundle.Preprocessor)
	write("calibration.json", bundle.Calibration)
	write("decision_policy.json", bundle.Policy)
	artifacts := make(map[string]string)
	for _, name := range []string{"scoring_contract.json", "preprocessing.json", "calibration.json", "decision_policy.json"} {
		hash, err := fileSHA256(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		artifacts[name] = hash
	}
	write("artifact_manifest.json", artifactManifest{RunID: bundle.Contract.RunID, ModelName: bundle.Contract.ModelName, Artifacts: artifacts})
	if _, err := LoadArtifactBundle(root); err != nil {
		t.Fatalf("valid bundle failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "calibration.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadArtifactBundle(root); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("tampered bundle should fail, got %v", err)
	}
}

func TestHTTPServiceScoresCompleteHandAndExportsMetrics(t *testing.T) {
	backend := &fakeBackend{}
	scorer, err := NewScorer(testBundle(t), backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	assembler, _ := NewHandAssembler(15, time.Hour)
	service, _ := NewHTTPService(scorer, assembler, time.Second)
	payload, _ := json.Marshal(scoreHandRequest{Pairs: testEvents()})
	request := httptest.NewRequest(http.MethodPost, "/v1/score-hand", bytes.NewReader(payload))
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"alert":true`) {
		t.Fatalf("unexpected score response: status=%d body=%s", response.Code, response.Body.String())
	}
	metricsRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsResponse := httptest.NewRecorder()
	service.Handler().ServeHTTP(metricsResponse, metricsRequest)
	if !strings.Contains(metricsResponse.Body.String(), "risk_scorer_pairs_scored_total 15") {
		t.Fatalf("unexpected metrics: %s", metricsResponse.Body.String())
	}
	if !strings.Contains(metricsResponse.Body.String(), "risk_scorer_request_duration_seconds_count 1") {
		t.Fatalf("request histogram was not exported: %s", metricsResponse.Body.String())
	}
}

func TestHTTPServiceRejectsTenantOutsideAllowlist(t *testing.T) {
	backend := &fakeBackend{}
	scorer, _ := NewScorer(testBundle(t), backend, nil)
	assembler, _ := NewHandAssembler(15, time.Hour)
	service, _ := NewHTTPService(scorer, assembler, time.Second)
	if err := service.SetAllowedTenants([]string{"tenant-a"}); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(scoreHandRequest{Pairs: testEvents()})
	request := httptest.NewRequest(http.MethodPost, "/v1/score-hand", bytes.NewReader(payload))
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || backend.calls != 0 {
		t.Fatalf("unauthorized tenant reached scorer: status=%d calls=%d", response.Code, backend.calls)
	}
}

type concurrentBackend struct {
	calls atomic.Int64
}

func (backend *concurrentBackend) Infer(_ context.Context, _, _ string, rows [][]float32) ([][]float32, error) {
	backend.calls.Add(1)
	result := make([][]float32, len(rows))
	for index := range result {
		result[index] = []float32{0.8, 0.2}
	}
	return result, nil
}

func (*concurrentBackend) Ready(context.Context) error { return nil }

func TestHTTPServiceConcurrentLoad(t *testing.T) {
	backend := &concurrentBackend{}
	scorer, _ := NewScorer(testBundle(t), backend, nil)
	assembler, _ := NewHandAssembler(15, time.Hour)
	service, _ := NewHTTPService(scorer, assembler, time.Second)
	payload, _ := json.Marshal(scoreHandRequest{Pairs: testEvents()})
	const requests = 128
	errors := make(chan error, requests)
	var wait sync.WaitGroup
	for index := 0; index < requests; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			request := httptest.NewRequest(http.MethodPost, "/v1/score-hand", bytes.NewReader(payload))
			response := httptest.NewRecorder()
			service.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				errors <- fmt.Errorf("unexpected status %d", response.Code)
			}
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	if backend.calls.Load() != requests || service.metrics.handsScored.Load() != requests {
		t.Fatalf("load run lost requests: backend=%d hands=%d", backend.calls.Load(), service.metrics.handsScored.Load())
	}
	if service.metrics.inflight.Load() != 0 || service.metrics.requestCount.Load() != requests {
		t.Fatalf("load metrics are inconsistent")
	}
}

func BenchmarkScoreHand(b *testing.B) {
	bundle := testBundle(b)
	backend := &concurrentBackend{}
	scorer, err := NewScorer(bundle, backend, nil)
	if err != nil {
		b.Fatal(err)
	}
	events := testEvents()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := scorer.ScoreHand(context.Background(), events); err != nil {
			b.Fatal(err)
		}
	}
}

func TestProfilingAddressMustBeExplicitLoopback(t *testing.T) {
	for _, address := range []string{"127.0.0.1:6060", "[::1]:6060", "localhost:6060"} {
		if err := ValidateProfilingAddress(address); err != nil {
			t.Fatalf("valid loopback %s rejected: %v", address, err)
		}
	}
	for _, address := range []string{":6060", "0.0.0.0:6060", "192.0.2.1:6060"} {
		if err := ValidateProfilingAddress(address); err == nil {
			t.Fatalf("unsafe profiling address %s accepted", address)
		}
	}
}
