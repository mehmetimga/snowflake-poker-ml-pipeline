package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type CollisionError struct {
	EventID string
}

func (errorValue *CollisionError) Error() string {
	return fmt.Sprintf("immutable event ID collision: %s", errorValue.EventID)
}

type HTTPPersister struct {
	baseURL string
	client  *http.Client
}

func NewHTTPPersister(baseURL string, client *http.Client) (*HTTPPersister, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("Snowflake writer URL is required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPPersister{baseURL: baseURL, client: client}, nil
}

func (persister *HTTPPersister) Ready(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, persister.baseURL+"/healthz", nil)
	if err != nil {
		return err
	}
	response, err := persister.client.Do(request)
	if err != nil {
		return fmt.Errorf("Snowflake writer readiness: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Snowflake writer readiness status: %d", response.StatusCode)
	}
	return nil
}

func (persister *HTTPPersister) Persist(
	ctx context.Context, requestValue PersistRequest,
) (PersistResult, error) {
	body, err := json.Marshal(requestValue)
	if err != nil {
		return PersistResult{}, fmt.Errorf("encode persistence request: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, persister.baseURL+"/v1/events/persist", bytes.NewReader(body),
	)
	if err != nil {
		return PersistResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := persister.client.Do(request)
	if err != nil {
		return PersistResult{}, fmt.Errorf("Snowflake writer request: %w", err)
	}
	defer response.Body.Close()
	responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if response.StatusCode == http.StatusConflict {
		return PersistResult{}, &CollisionError{EventID: requestValue.EventID}
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return PersistResult{}, fmt.Errorf("Snowflake writer status: %d", response.StatusCode)
	}
	var result PersistResult
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return PersistResult{}, fmt.Errorf("decode Snowflake writer response: %w", err)
	}
	return result, nil
}
