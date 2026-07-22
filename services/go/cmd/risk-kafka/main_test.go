package main

import "testing"

func simulationTopicMap() map[string]string {
	return map[string]string{
		"input": "poker.sim.pair-features.v1", "scores": "poker.sim.risk-scores.v1",
		"rules": "poker.sim.rule-evidence.v1", "decisions": "poker.sim.review-decisions.v1",
		"alerts": "poker.sim.risk-alerts.v1", "dead_letter": "poker.sim.pipeline.dead-letter.v1",
	}
}

func TestSimulationKafkaBoundaryRequiresExactTopicsAndGroup(t *testing.T) {
	topics := simulationTopicMap()
	if err := validateKafkaBoundary(true, "poker-go-risk-scorer-sim-v1", topics); err != nil {
		t.Fatalf("valid simulation boundary rejected: %v", err)
	}
	if err := validateKafkaBoundary(true, "poker-go-risk-scorer-v1", topics); err == nil {
		t.Fatal("simulation mode accepted the production group")
	}
	topics["scores"] = "poker.risk-scores.v1"
	if err := validateKafkaBoundary(true, "poker-go-risk-scorer-sim-v1", topics); err == nil {
		t.Fatal("simulation mode accepted a production topic")
	}
}

func TestProductionKafkaBoundaryRejectsSimulationTopic(t *testing.T) {
	if err := validateKafkaBoundary(false, "poker-go-risk-scorer-v1", simulationTopicMap()); err == nil {
		t.Fatal("production mode accepted simulation topics")
	}
}
