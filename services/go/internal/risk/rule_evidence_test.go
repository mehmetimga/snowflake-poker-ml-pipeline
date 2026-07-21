package risk

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func ruleEvidenceFixture(t *testing.T) RuleEvidenceEvent {
	t.Helper()
	value, err := os.ReadFile("../../../../schemas/examples/poker.rule-evidence.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var event RuleEvidenceEvent
	if err := json.Unmarshal(value, &event); err != nil {
		t.Fatal(err)
	}
	return event
}

func TestRuleEvidenceFixtureValidatesAndRebuildsAcrossLanguages(t *testing.T) {
	fixture := ruleEvidenceFixture(t)
	if err := fixture.Validate(); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := BuildRuleEvidenceEvent(RuleEvidenceInput{
		TenantID: fixture.TenantID, ProductID: fixture.ProductID,
		DatasetID: fixture.DatasetID, DatasetSplit: fixture.DatasetSplit,
		TraceID: fixture.TraceID, RuleID: fixture.Payload.RuleID,
		RuleVersion: fixture.Payload.RuleVersion, RuleOwner: fixture.Payload.RuleOwner,
		EntityType: fixture.Payload.EntityType, EntityKey: fixture.Payload.EntityKey,
		HandID: fixture.Payload.HandID, Severity: fixture.Payload.Severity,
		RawScore: fixture.Payload.RawScore, Evidence: fixture.Payload.Evidence,
		EffectiveAt: fixture.Payload.EffectiveAt, EmittedAt: fixture.EmittedAt,
		FeatureDefinitionVersion: fixture.Payload.FeatureDefinitionVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rebuilt, fixture) {
		t.Fatalf("rebuilt event differs from shared fixture:\nrebuilt=%+v\nfixture=%+v", rebuilt, fixture)
	}
	second, err := BuildRuleEvidenceEvent(RuleEvidenceInput{
		TenantID: fixture.TenantID, ProductID: fixture.ProductID,
		DatasetID: fixture.DatasetID, DatasetSplit: fixture.DatasetSplit,
		TraceID: fixture.TraceID, RuleID: fixture.Payload.RuleID,
		RuleVersion: fixture.Payload.RuleVersion, RuleOwner: fixture.Payload.RuleOwner,
		EntityType: fixture.Payload.EntityType, EntityKey: fixture.Payload.EntityKey,
		HandID: fixture.Payload.HandID, Severity: fixture.Payload.Severity,
		RawScore: fixture.Payload.RawScore, Evidence: fixture.Payload.Evidence,
		EffectiveAt: fixture.Payload.EffectiveAt, EmittedAt: fixture.EmittedAt,
		FeatureDefinitionVersion: fixture.Payload.FeatureDefinitionVersion,
	})
	if err != nil || second.EventID != rebuilt.EventID {
		t.Fatal("replayed rule evidence did not preserve its deterministic event ID")
	}
}

func TestRuleEvidenceRejectsPrivateAndDecisionOutputs(t *testing.T) {
	fixture := ruleEvidenceFixture(t)
	fixture.Payload.Evidence["nested"] = map[string]any{
		"hand_risk_probability": 0.99,
	}
	err := fixture.Validate()
	if err == nil || !strings.Contains(err.Error(), "forbidden rule-evidence field") {
		t.Fatalf("expected forbidden-field rejection, got %v", err)
	}
}
