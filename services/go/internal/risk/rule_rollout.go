package risk

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const statefulFoldRuleID = "pair.repeated-fold-to-partner-wins"

type RuleRolloutEntry struct {
	RuleID      string `json:"rule_id"`
	RuleVersion int    `json:"rule_version"`
	Runtime     string `json:"runtime"`
	Enabled     bool   `json:"enabled"`
}

type RuleRollbackPolicy struct {
	PreserveHistoricalEvidence            bool `json:"preserve_historical_evidence"`
	DeleteHistoricalEvidence              bool `json:"delete_historical_evidence"`
	ModelInferenceReconfigurationRequired bool `json:"model_inference_reconfiguration_required"`
	ModelProbabilityMustMatchBitForBit    bool `json:"model_probability_must_match_bit_for_bit"`
}

type RuleRolloutConfig struct {
	SchemaVersion int                `json:"schema_version"`
	RolloutID     string             `json:"rollout_id"`
	RuleSet       string             `json:"rule_set"`
	Mode          string             `json:"mode"`
	Rules         []RuleRolloutEntry `json:"rules"`
	Rollback      RuleRollbackPolicy `json:"rollback"`
}

func LoadRuleRollout(path string) (*RuleRolloutConfig, error) {
	value, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config RuleRolloutConfig
	if err := json.Unmarshal(value, &config); err != nil {
		return nil, fmt.Errorf("decode rule rollout: %w", err)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func (config RuleRolloutConfig) Validate() error {
	if config.SchemaVersion != 1 || strings.TrimSpace(config.RolloutID) == "" ||
		config.RuleSet != "rules-v2" || config.Mode != "shadow" {
		return fmt.Errorf("rule rollout must be schema v1 Rules v2 shadow mode")
	}
	expected := map[string]int{statefulFoldRuleID: 1}
	for _, definition := range pairRuleDefinitions {
		expected[definition.RuleID] = definition.RuleVersion
	}
	seen := make(map[string]struct{}, len(config.Rules))
	for _, entry := range config.Rules {
		version, exists := expected[entry.RuleID]
		if !exists || entry.RuleVersion != version || strings.TrimSpace(entry.Runtime) == "" {
			return fmt.Errorf("unknown or mismatched rollout rule %s:v%d", entry.RuleID, entry.RuleVersion)
		}
		expectedRuntime := "go-risk-scorer"
		if entry.RuleID == statefulFoldRuleID {
			expectedRuntime = "java-flink-pair-features"
		}
		if entry.Runtime != expectedRuntime {
			return fmt.Errorf("rollout rule %s must run in %s", entry.RuleID, expectedRuntime)
		}
		if _, duplicate := seen[entry.RuleID]; duplicate {
			return fmt.Errorf("duplicate rollout rule %s", entry.RuleID)
		}
		seen[entry.RuleID] = struct{}{}
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("rollout must exactly cover all seven governed rules")
	}
	if !config.Rollback.PreserveHistoricalEvidence || config.Rollback.DeleteHistoricalEvidence ||
		config.Rollback.ModelInferenceReconfigurationRequired ||
		!config.Rollback.ModelProbabilityMustMatchBitForBit {
		return fmt.Errorf("rollback policy violates immutable evidence or probability invariance")
	}
	return nil
}

func (config RuleRolloutConfig) GoRuleEnablement() (map[string]bool, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	enabled := make(map[string]bool, len(pairRuleDefinitions))
	for _, entry := range config.Rules {
		if entry.Runtime == "go-risk-scorer" {
			enabled[entry.RuleID] = entry.Enabled
		}
	}
	for _, definition := range pairRuleDefinitions {
		if _, exists := enabled[definition.RuleID]; !exists {
			return nil, fmt.Errorf("Go rule %s is assigned to the wrong runtime", definition.RuleID)
		}
	}
	return enabled, nil
}
