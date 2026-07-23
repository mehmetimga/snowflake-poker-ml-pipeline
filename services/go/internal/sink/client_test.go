package sink

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func TestHTTPPersisterInsertedAndReady(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(
		request *http.Request,
	) (*http.Response, error) {
		if request.URL.Path == "/healthz" {
			return response(http.StatusOK, ""), nil
		}
		if request.URL.Path != "/v1/events/persist" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
		return response(http.StatusCreated, `{"status":"inserted"}`), nil
	})}
	persister, err := NewHTTPPersister("http://writer", client)
	if err != nil {
		t.Fatal(err)
	}
	if err := persister.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := persister.Persist(context.Background(), PersistRequest{EventID: "event-1"})
	if err != nil || result.Status != "inserted" {
		t.Fatalf("unexpected result=%+v err=%v", result, err)
	}
}

func TestHTTPPersisterMapsCollision(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(
		_ *http.Request,
	) (*http.Response, error) {
		return response(http.StatusConflict, ""), nil
	})}
	persister, _ := NewHTTPPersister("http://writer", client)
	_, err := persister.Persist(context.Background(), PersistRequest{EventID: "event-1"})
	var collision *CollisionError
	if !errors.As(err, &collision) || collision.EventID != "event-1" {
		t.Fatalf("expected collision error, got %v", err)
	}
}
