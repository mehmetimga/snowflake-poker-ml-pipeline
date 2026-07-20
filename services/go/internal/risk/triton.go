package risk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type TritonBackend struct {
	baseURL string
	model   string
	client  *http.Client
}

func NewTritonBackend(baseURL, model string, client *http.Client) (*TritonBackend, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid Triton base URL %q", baseURL)
	}
	if model == "" {
		return nil, fmt.Errorf("Triton model name is required")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &TritonBackend{baseURL: baseURL, model: model, client: client}, nil
}

type tritonInput struct {
	Name     string    `json:"name"`
	Shape    []int     `json:"shape"`
	Datatype string    `json:"datatype"`
	Data     []float32 `json:"data"`
}

type tritonOutputRequest struct {
	Name string `json:"name"`
}

type tritonRequest struct {
	Inputs  []tritonInput         `json:"inputs"`
	Outputs []tritonOutputRequest `json:"outputs"`
}

type tritonOutput struct {
	Name     string    `json:"name"`
	Shape    []int     `json:"shape"`
	Datatype string    `json:"datatype"`
	Data     []float32 `json:"data"`
}

type tritonResponse struct {
	ModelName string         `json:"model_name"`
	Outputs   []tritonOutput `json:"outputs"`
	Error     string         `json:"error"`
}

func (backend *TritonBackend) Infer(ctx context.Context, inputName, outputName string, rows [][]float32) ([][]float32, error) {
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, fmt.Errorf("Triton input must not be empty")
	}
	width := len(rows[0])
	data := make([]float32, 0, len(rows)*width)
	for index, row := range rows {
		if len(row) != width {
			return nil, fmt.Errorf("Triton input row %d has inconsistent width", index)
		}
		data = append(data, row...)
	}
	payload := tritonRequest{
		Inputs:  []tritonInput{{Name: inputName, Shape: []int{len(rows), width}, Datatype: "FP32", Data: data}},
		Outputs: []tritonOutputRequest{{Name: outputName}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	endpoint := backend.baseURL + "/v2/models/" + url.PathEscape(backend.model) + "/infer"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := backend.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, 16<<20)
	var decoded tritonResponse
	if err := json.NewDecoder(limited).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode Triton response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("Triton returned %s: %s", response.Status, decoded.Error)
	}
	for _, output := range decoded.Outputs {
		if output.Name != outputName {
			continue
		}
		if output.Datatype != "FP32" || len(output.Shape) != 2 || output.Shape[0] != len(rows) || output.Shape[1] != 2 || len(output.Data) != len(rows)*2 {
			return nil, fmt.Errorf("Triton output shape or datatype is invalid")
		}
		result := make([][]float32, len(rows))
		for index := range rows {
			result[index] = append([]float32(nil), output.Data[index*2:index*2+2]...)
		}
		return result, nil
	}
	return nil, fmt.Errorf("Triton response did not contain output %s", outputName)
}

func (backend *TritonBackend) Ready(ctx context.Context) error {
	endpoint := backend.baseURL + "/v2/models/" + url.PathEscape(backend.model) + "/ready"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	response, err := backend.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Triton model is not ready: %s", response.Status)
	}
	return nil
}
