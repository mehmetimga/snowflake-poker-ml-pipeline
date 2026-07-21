package risk

import (
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
	"time"
)

type pairRulesGolden struct {
	Input struct {
		EventID          string         `json:"event_id"`
		TenantID         string         `json:"tenant_id"`
		ProductID        string         `json:"product_id"`
		DatasetID        string         `json:"dataset_id"`
		DatasetSplit     string         `json:"dataset_split"`
		TraceID          string         `json:"trace_id"`
		HandID           string         `json:"hand_id"`
		TableID          string         `json:"table_id"`
		PairKey          string         `json:"pair_key"`
		PlayerA          string         `json:"player_a"`
		PlayerB          string         `json:"player_b"`
		PlayedAt         string         `json:"played_at"`
		EmittedAt        string         `json:"emitted_at"`
		RuleEmittedAt    string         `json:"rule_emitted_at"`
		SnapshotRevision int            `json:"snapshot_revision"`
		CurrentHand      map[string]any `json:"current_hand"`
		Context          map[string]any `json:"context"`
		PairHistory      map[string]any `json:"pair_history"`
	} `json:"input"`
	Expected struct {
		RulesOnlyScore float64 `json:"rules_only_score"`
		FiredRules     []struct {
			RuleID      string  `json:"rule_id"`
			RawScore    float64 `json:"raw_score"`
			RuleEventID string  `json:"rule_event_id"`
		} `json:"fired_rules"`
	} `json:"expected"`
}

func loadPairRulesGolden(t *testing.T) pairRulesGolden {
	t.Helper()
	value, err := os.ReadFile("../../../../schemas/examples/pair-rules-v1.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture pairRulesGolden
	if err := json.Unmarshal(value, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func goldenPairRuleEvent(t *testing.T, fixture pairRulesGolden) PairFeatureEvent {
	t.Helper()
	version := 1
	input := fixture.Input
	return PairFeatureEvent{
		EventID: input.EventID, EventType: pairFeatureEventType, SchemaVersion: 1,
		TenantID: input.TenantID, ProductID: input.ProductID,
		DatasetID: input.DatasetID, DatasetSplit: input.DatasetSplit,
		OccurredAt: input.PlayedAt, EmittedAt: input.EmittedAt, TraceID: input.TraceID,
		Payload: PairFeaturePayload{
			HandID: input.HandID, TableID: input.TableID, PlayedAt: input.PlayedAt,
			PairKey: input.PairKey, PlayerA: input.PlayerA, PlayerB: input.PlayerB, NumPlayers: 6,
			SourceHandEventID: "source-hand", SourcePlayerContextEventIDA: "context-a", SourcePlayerContextEventIDB: "context-b",
			SourceRevisionA: 1, SourceRevisionB: 1,
			ContextStatusA: "matched", ContextStatusB: "matched",
			ContextVersionA: &version, ContextVersionB: &version,
			SnapshotRevision: input.SnapshotRevision, FeatureDefinitionVersion: ruleFeatureVersion,
			CurrentHand: input.CurrentHand, Context: input.Context,
			UserHistoryA: map[string]any{}, UserHistoryB: map[string]any{}, PairHistory: input.PairHistory,
		},
	}
}

func TestPairRuleDefinitionsMatchGovernedFile(t *testing.T) {
	value, err := os.ReadFile("../../../../schemas/rules/pair-rules-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var governed struct {
		FeatureDefinitionVersion string               `json:"feature_definition_version"`
		Rules                    []PairRuleDefinition `json:"rules"`
	}
	if err := json.Unmarshal(value, &governed); err != nil {
		t.Fatal(err)
	}
	if governed.FeatureDefinitionVersion != ruleFeatureVersion || !reflect.DeepEqual(governed.Rules, PairRuleDefinitions()) {
		t.Fatalf("Go rule definitions drifted from governed JSON")
	}
}

func TestRuleRolloutExactlyCoversGovernedRules(t *testing.T) {
	config, err := LoadRuleRollout("../../../../schemas/rules/rule-rollout-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	enabled, err := config.GoRuleEnablement()
	if err != nil {
		t.Fatal(err)
	}
	if config.RolloutID != "rules-v2-shadow-v1" || len(enabled) != len(PairRuleDefinitions()) {
		t.Fatalf("unexpected rollout identity or Go rule count: %+v", config)
	}
	for _, definition := range PairRuleDefinitions() {
		if !enabled[definition.RuleID] {
			t.Fatalf("governed baseline unexpectedly disables %s", definition.RuleID)
		}
	}
}

func TestPairRulesMatchCrossLanguageGoldenFixture(t *testing.T) {
	fixture := loadPairRulesGolden(t)
	event := goldenPairRuleEvent(t, fixture)
	emittedAt, err := time.Parse(time.RFC3339Nano, fixture.Input.RuleEmittedAt)
	if err != nil {
		t.Fatal(err)
	}
	fired, err := EvaluatePairRules(event, emittedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != len(fixture.Expected.FiredRules) {
		t.Fatalf("got %d rules, want %d", len(fired), len(fixture.Expected.FiredRules))
	}
	for index, expected := range fixture.Expected.FiredRules {
		actual := fired[index]
		if actual.Payload.RuleID != expected.RuleID || actual.Payload.RawScore != expected.RawScore || actual.EventID != expected.RuleEventID {
			t.Fatalf("rule %d mismatch: %+v want %+v", index, actual.Payload, expected)
		}
	}
	score, err := RulesOnlyPairScore(event)
	if err != nil || math.Abs(score-fixture.Expected.RulesOnlyScore) > 1e-12 {
		t.Fatalf("rules-only score=%v err=%v", score, err)
	}
	replay, err := EvaluatePairRules(event, emittedAt.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for index := range fired {
		if fired[index].EventID != replay[index].EventID {
			t.Fatal("replay changed deterministic rule evidence ID")
		}
	}
	corrected := event
	corrected.Payload.SnapshotRevision = 4
	correctedRules, err := EvaluatePairRules(corrected, emittedAt)
	if err != nil {
		t.Fatal(err)
	}
	for index := range fired {
		if fired[index].EventID == correctedRules[index].EventID || correctedRules[index].Payload.ObservationRevision != 4 {
			t.Fatal("higher source revision must produce distinct revision-aware evidence")
		}
	}
}

func TestPairRulesRequireAllSixInferenceSafeSignals(t *testing.T) {
	fixture := loadPairRulesGolden(t)
	event := goldenPairRuleEvent(t, fixture)
	delete(event.Payload.PairHistory, "outcome_asymmetry")
	if _, err := RulesOnlyPairScore(event); err == nil {
		t.Fatal("missing governed rule input must fail closed")
	}
}
