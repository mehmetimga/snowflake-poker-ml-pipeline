package risk

import (
	"encoding/json"
	"os"
	"testing"
)

type alertAcceptanceIdentityGolden struct {
	ModelRunID          string `json:"model_run_id"`
	TenantID            string `json:"tenant_id"`
	ProductID           string `json:"product_id"`
	DatasetID           string `json:"dataset_id"`
	DatasetSplit        string `json:"dataset_split"`
	ReviewPolicyID      string `json:"review_policy_id"`
	ReviewPolicyVersion int    `json:"review_policy_version"`
	Features            []struct {
		EventID          string `json:"event_id"`
		PairKey          string `json:"pair_key"`
		SnapshotRevision int    `json:"snapshot_revision"`
	} `json:"features"`
	Expected struct {
		ScoreID               string `json:"score_id"`
		RiskScoreEventID      string `json:"risk_score_event_id"`
		ReviewDecisionEventID string `json:"review_decision_event_id"`
		RiskAlertEventID      string `json:"risk_alert_event_id"`
	} `json:"expected"`
}

func TestAlertAcceptanceIdentitiesMatchPythonOracle(t *testing.T) {
	value, err := os.ReadFile("../../../../schemas/examples/alert-acceptance-identities-v1.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture alertAcceptanceIdentityGolden
	if err := json.Unmarshal(value, &fixture); err != nil {
		t.Fatal(err)
	}
	events := make([]PairFeatureEvent, 0, len(fixture.Features))
	for _, feature := range fixture.Features {
		events = append(events, PairFeatureEvent{
			EventID: feature.EventID,
			Payload: PairFeaturePayload{
				PairKey:          feature.PairKey,
				SnapshotRevision: feature.SnapshotRevision,
			},
		})
	}
	scoreID := scoreIdentity(fixture.ModelRunID, events)
	if scoreID != fixture.Expected.ScoreID {
		t.Fatalf("score identity mismatch: got=%s want=%s", scoreID, fixture.Expected.ScoreID)
	}
	scoreEventID := stableUUID(RiskScoreEventType, scoreID)
	if scoreEventID != fixture.Expected.RiskScoreEventID {
		t.Fatalf("score event identity mismatch: got=%s want=%s", scoreEventID, fixture.Expected.RiskScoreEventID)
	}
	decisionID := stableReviewDecisionID(
		fixture.TenantID,
		fixture.ProductID,
		fixture.DatasetID,
		fixture.DatasetSplit,
		fixture.ReviewPolicyID,
		fixture.ReviewPolicyVersion,
		scoreEventID,
	)
	if decisionID != fixture.Expected.ReviewDecisionEventID {
		t.Fatalf("review decision identity mismatch: got=%s want=%s", decisionID, fixture.Expected.ReviewDecisionEventID)
	}
	alertID := stableUUID(RiskAlertEventType, scoreID, decisionID)
	if alertID != fixture.Expected.RiskAlertEventID {
		t.Fatalf("alert identity mismatch: got=%s want=%s", alertID, fixture.Expected.RiskAlertEventID)
	}
}
