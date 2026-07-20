package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/risk"
)

func main() {
	modelDir := flag.String("model-dir", "../../models/pair-catboost-full-v2", "model artifact directory")
	flag.Parse()
	bundle, err := risk.LoadArtifactBundle(*modelDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[risk-contract-check] failed: %v\n", err)
		os.Exit(1)
	}
	result := map[string]any{
		"model_name":                 bundle.Contract.ModelName,
		"run_id":                     bundle.Contract.RunID,
		"feature_definition_version": bundle.Contract.FeatureDefinitionVersion,
		"input_features":             len(bundle.Contract.Input.OrderedFeatures),
		"pairs_per_hand":             bundle.Contract.Batching.ExpectedPairsPerSixPlayerHand,
		"triton_model":               bundle.Contract.Batching.TritonModel,
		"decision_threshold":         bundle.Policy.Threshold,
		"artifact_hashes":            "passed",
		"contract":                   "passed",
	}
	encoded, _ := json.Marshal(result)
	fmt.Printf("[risk-contract-check] %s\n", encoded)
}
