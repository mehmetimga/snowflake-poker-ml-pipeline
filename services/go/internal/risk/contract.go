package risk

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	missingCategory = "__MISSING__"
	unknownCategory = "__UNKNOWN__"
)

type ScoringContract struct {
	ContractVersion          int    `json:"contract_version"`
	ModelName                string `json:"model_name"`
	RunID                    string `json:"run_id"`
	FeatureDefinitionVersion string `json:"feature_definition_version"`
	Input                    struct {
		Name            string   `json:"name"`
		DType           string   `json:"dtype"`
		Shape           []*int   `json:"shape"`
		Preprocessing   string   `json:"preprocessing"`
		OrderedFeatures []string `json:"ordered_features"`
	} `json:"input"`
	Output struct {
		Name                       string `json:"name"`
		DType                      string `json:"dtype"`
		Shape                      []*int `json:"shape"`
		PositiveClassIndex         int    `json:"positive_class_index"`
		ProbabilitiesAreCalibrated bool   `json:"probabilities_are_calibrated"`
	} `json:"output"`
	Calibration    string `json:"calibration"`
	DecisionPolicy string `json:"decision_policy"`
	Batching       struct {
		Unit                          string `json:"unit"`
		ExpectedPairsPerSixPlayerHand int    `json:"expected_pairs_per_six_player_hand"`
		TritonModel                   string `json:"triton_model"`
	} `json:"batching"`
}

type PreprocessingContract struct {
	ContractVersion    int                 `json:"contract_version"`
	NumericColumns     []string            `json:"numeric_columns"`
	CategoricalColumns []string            `json:"categorical_columns"`
	NumericFillValues  map[string]float64  `json:"numeric_fill_values"`
	CategoricalValues  map[string][]string `json:"categorical_values"`
	OutputColumns      []string            `json:"output_columns"`
	OutputDType        string              `json:"output_dtype"`
}

type CalibrationContract struct {
	Slope     float64 `json:"slope"`
	Intercept float64 `json:"intercept"`
	Method    string  `json:"method"`
}

type DecisionPolicy struct {
	PolicyVersion          int     `json:"policy_version"`
	Probability            string  `json:"probability"`
	Threshold              float64 `json:"threshold"`
	ValidationMaxAlertRate float64 `json:"validation_max_alert_rate"`
	PairsPerSixPlayerHand  int     `json:"pairs_per_six_player_hand"`
	Aggregation            struct {
		Player string `json:"player"`
		Hand   string `json:"hand"`
	} `json:"aggregation"`
}

type artifactManifest struct {
	RunID     string            `json:"run_id"`
	ModelName string            `json:"model_name"`
	Artifacts map[string]string `json:"artifacts"`
}

type ArtifactBundle struct {
	Root         string
	Contract     ScoringContract
	Preprocessor PreprocessingContract
	Calibration  CalibrationContract
	Policy       DecisionPolicy
}

func LoadArtifactBundle(root string) (*ArtifactBundle, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve model directory: %w", err)
	}
	var manifest artifactManifest
	if err := readJSON(filepath.Join(absRoot, "artifact_manifest.json"), &manifest); err != nil {
		return nil, err
	}
	if len(manifest.Artifacts) == 0 {
		return nil, fmt.Errorf("artifact manifest is empty")
	}
	for relative, expected := range manifest.Artifacts {
		clean := filepath.Clean(relative)
		if filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("unsafe artifact path %q", relative)
		}
		actual, err := fileSHA256(filepath.Join(absRoot, clean))
		if err != nil {
			return nil, fmt.Errorf("verify artifact %s: %w", relative, err)
		}
		if !strings.EqualFold(actual, expected) {
			return nil, fmt.Errorf("artifact hash mismatch: %s", relative)
		}
	}

	bundle := &ArtifactBundle{Root: absRoot}
	if err := readJSON(filepath.Join(absRoot, "scoring_contract.json"), &bundle.Contract); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(absRoot, bundle.Contract.Input.Preprocessing), &bundle.Preprocessor); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(absRoot, bundle.Contract.Calibration), &bundle.Calibration); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(absRoot, bundle.Contract.DecisionPolicy), &bundle.Policy); err != nil {
		return nil, err
	}
	if err := bundle.Validate(); err != nil {
		return nil, err
	}
	if manifest.RunID != bundle.Contract.RunID || manifest.ModelName != bundle.Contract.ModelName {
		return nil, fmt.Errorf("artifact manifest identity does not match scoring contract")
	}
	return bundle, nil
}

func (b *ArtifactBundle) Validate() error {
	if b.Contract.ContractVersion != 1 || b.Preprocessor.ContractVersion != 1 || b.Policy.PolicyVersion != 1 {
		return fmt.Errorf("unsupported model contract version")
	}
	if b.Contract.ModelName == "" || b.Contract.RunID == "" || b.Contract.FeatureDefinitionVersion == "" {
		return fmt.Errorf("model identity is incomplete")
	}
	if b.Contract.Input.DType != "float32" || b.Contract.Output.DType != "float32" || b.Preprocessor.OutputDType != "float32" {
		return fmt.Errorf("only float32 model contracts are supported")
	}
	if len(b.Contract.Input.Shape) != 2 || b.Contract.Input.Shape[1] == nil || *b.Contract.Input.Shape[1] != len(b.Preprocessor.OutputColumns) {
		return fmt.Errorf("model input shape does not match preprocessing output")
	}
	if len(b.Contract.Output.Shape) != 2 || b.Contract.Output.Shape[1] == nil || *b.Contract.Output.Shape[1] != 2 {
		return fmt.Errorf("model output must contain two class probabilities")
	}
	if b.Contract.Output.PositiveClassIndex != 1 || b.Contract.Output.ProbabilitiesAreCalibrated {
		return fmt.Errorf("unsupported probability output contract")
	}
	if !equalStrings(b.Contract.Input.OrderedFeatures, b.Preprocessor.OutputColumns) {
		return fmt.Errorf("ordered model features do not match preprocessing output")
	}
	expectedOutput := append([]string(nil), b.Preprocessor.NumericColumns...)
	for _, column := range b.Preprocessor.CategoricalColumns {
		for _, value := range b.Preprocessor.CategoricalValues[column] {
			expectedOutput = append(expectedOutput, column+"=="+value)
		}
	}
	if !equalStrings(expectedOutput, b.Preprocessor.OutputColumns) {
		return fmt.Errorf("preprocessing output columns do not follow the numeric and categorical contract order")
	}
	if b.Contract.Batching.Unit != "hand" || b.Contract.Batching.ExpectedPairsPerSixPlayerHand != 15 || b.Policy.PairsPerSixPlayerHand != 15 {
		return fmt.Errorf("risk scorer requires 15-pair six-player hand batches")
	}
	if b.Contract.Batching.TritonModel == "" || b.Contract.Input.Name == "" || b.Contract.Output.Name == "" {
		return fmt.Errorf("inference endpoint names are incomplete")
	}
	if b.Calibration.Slope <= 0 || b.Policy.Threshold <= 0 || b.Policy.Threshold >= 1 {
		return fmt.Errorf("invalid calibration or decision threshold")
	}
	if b.Policy.Aggregation.Player != "max_pair_probability" || b.Policy.Aggregation.Hand != "max_pair_probability" {
		return fmt.Errorf("unsupported aggregation policy")
	}
	for _, column := range b.Preprocessor.NumericColumns {
		if _, ok := b.Preprocessor.NumericFillValues[column]; !ok {
			return fmt.Errorf("numeric fill value missing for %s", column)
		}
	}
	for _, column := range b.Preprocessor.CategoricalColumns {
		values := b.Preprocessor.CategoricalValues[column]
		if len(values) == 0 || values[len(values)-1] != unknownCategory {
			return fmt.Errorf("categorical values for %s must end with %s", column, unknownCategory)
		}
	}
	return nil
}

func readJSON(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	value, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
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
