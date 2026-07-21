package risk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type serviceMetrics struct {
	requests        atomic.Int64
	errors          atomic.Int64
	handsScored     atomic.Int64
	pairsScored     atomic.Int64
	latencyMicros   atomic.Int64
	requestMicros   atomic.Int64
	requestCount    atomic.Int64
	inflight        atomic.Int64
	readyFailures   atomic.Int64
	requestBuckets  [9]atomic.Int64
	lastSuccessUnix atomic.Int64
	dynamicMu       sync.Mutex
	scopeHands      map[scoringScope]int64
	scopePairs      map[scoringScope]int64
	ruleEvidence    map[ruleMetricKey]int64
}

type scoringScope struct {
	tenantID  string
	productID string
	modelName string
	modelRun  string
	rolloutID string
}

type ruleMetricKey struct {
	scoringScope
	ruleID      string
	ruleVersion int
}

var requestLatencyUpperMicros = [...]int64{1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000}

type HTTPService struct {
	scorer         *Scorer
	assembler      *HandAssembler
	metrics        serviceMetrics
	readyTimeout   time.Duration
	allowedTenants map[string]struct{}
}

// SetAllowedTenants installs an explicit tenant allowlist before serving.
// An empty list keeps the development default of accepting all valid tenants.
func (service *HTTPService) SetAllowedTenants(tenants []string) error {
	allowed := make(map[string]struct{}, len(tenants))
	for _, tenant := range tenants {
		tenant = strings.TrimSpace(tenant)
		if tenant == "" {
			return fmt.Errorf("allowed tenant IDs cannot be empty")
		}
		allowed[tenant] = struct{}{}
	}
	service.allowedTenants = allowed
	return nil
}

func (service *HTTPService) tenantAllowed(tenant string) bool {
	if len(service.allowedTenants) == 0 {
		return true
	}
	_, ok := service.allowedTenants[tenant]
	return ok
}

func (service *HTTPService) beginRequest() func() {
	service.metrics.requests.Add(1)
	service.metrics.inflight.Add(1)
	started := time.Now()
	return func() {
		elapsed := time.Since(started).Microseconds()
		service.metrics.inflight.Add(-1)
		service.metrics.requestMicros.Add(elapsed)
		service.metrics.requestCount.Add(1)
		for index, upper := range requestLatencyUpperMicros {
			if elapsed <= upper {
				service.metrics.requestBuckets[index].Add(1)
			}
		}
	}
}

func NewHTTPService(scorer *Scorer, assembler *HandAssembler, readyTimeout time.Duration) (*HTTPService, error) {
	if scorer == nil || assembler == nil || readyTimeout <= 0 {
		return nil, fmt.Errorf("scorer, assembler, and positive readiness timeout are required")
	}
	return &HTTPService{
		scorer: scorer, assembler: assembler, readyTimeout: readyTimeout,
		metrics: serviceMetrics{
			scopeHands:   make(map[scoringScope]int64),
			scopePairs:   make(map[scoringScope]int64),
			ruleEvidence: make(map[ruleMetricKey]int64),
		},
	}, nil
}

func (service *HTTPService) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", service.health)
	mux.HandleFunc("GET /readyz", service.ready)
	mux.HandleFunc("GET /metrics", service.prometheusMetrics)
	mux.HandleFunc("POST /v1/score-hand", service.scoreHand)
	mux.HandleFunc("POST /v1/pair-feature", service.addPairFeature)
	return mux
}

func (service *HTTPService) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{
		"status":                  "ok",
		"model_name":              service.scorer.bundle.Contract.ModelName,
		"model_run_id":            service.scorer.bundle.Contract.RunID,
		"decision_policy_version": service.scorer.bundle.Policy.PolicyVersion,
		"service_implementation":  "go-risk-scorer",
		"service_build_version":   service.scorer.serviceBuildVersion,
	})
}

func (service *HTTPService) ready(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), service.readyTimeout)
	defer cancel()
	if err := service.scorer.Ready(ctx); err != nil {
		service.metrics.readyFailures.Add(1)
		writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "error": err.Error()})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
}

type scoreHandRequest struct {
	Pairs []PairFeatureEvent `json:"pairs"`
}

func (service *HTTPService) scoreHand(writer http.ResponseWriter, request *http.Request) {
	done := service.beginRequest()
	defer done()
	var payload scoreHandRequest
	if err := decodeJSONRequest(writer, request, &payload); err != nil {
		service.metrics.errors.Add(1)
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	for _, event := range payload.Pairs {
		if !service.tenantAllowed(event.TenantID) {
			service.metrics.errors.Add(1)
			writeJSON(writer, http.StatusForbidden, map[string]string{"error": "tenant is not authorized"})
			return
		}
	}
	started := time.Now()
	result, err := service.scorer.ScoreHand(request.Context(), payload.Pairs)
	service.metrics.latencyMicros.Add(time.Since(started).Microseconds())
	if err != nil {
		service.metrics.errors.Add(1)
		writeJSON(writer, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	service.recordSuccess(result)
	writeJSON(writer, http.StatusOK, result)
}

func (service *HTTPService) addPairFeature(writer http.ResponseWriter, request *http.Request) {
	done := service.beginRequest()
	defer done()
	var event PairFeatureEvent
	if err := decodeJSONRequest(writer, request, &event); err != nil {
		service.metrics.errors.Add(1)
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if !service.tenantAllowed(event.TenantID) {
		service.metrics.errors.Add(1)
		writeJSON(writer, http.StatusForbidden, map[string]string{"error": "tenant is not authorized"})
		return
	}
	events, complete, err := service.assembler.Add(
		event,
		service.scorer.bundle.Contract.FeatureDefinitionVersion,
		time.Now(),
	)
	if err != nil {
		service.metrics.errors.Add(1)
		writeJSON(writer, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	if !complete {
		writeJSON(writer, http.StatusAccepted, map[string]any{
			"status":         "collecting",
			"hand_id":        event.Payload.HandID,
			"buffered_hands": service.assembler.Len(),
		})
		return
	}
	started := time.Now()
	result, err := service.scorer.ScoreHand(request.Context(), events)
	service.metrics.latencyMicros.Add(time.Since(started).Microseconds())
	if err != nil {
		service.metrics.errors.Add(1)
		writeJSON(writer, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	service.recordSuccess(result)
	writeJSON(writer, http.StatusOK, result)
}

func (service *HTTPService) recordSuccess(result *ScoreResult) {
	pairs := len(result.PairScores)
	service.metrics.handsScored.Add(1)
	service.metrics.pairsScored.Add(int64(pairs))
	service.metrics.lastSuccessUnix.Store(time.Now().Unix())
	rolloutID, _ := service.scorer.RuleRolloutSnapshot()
	scope := scoringScope{
		tenantID: result.TenantID, productID: result.ProductID,
		modelName: result.ModelName, modelRun: result.ModelRunID,
		rolloutID: rolloutID,
	}
	service.metrics.dynamicMu.Lock()
	defer service.metrics.dynamicMu.Unlock()
	service.metrics.scopeHands[scope]++
	service.metrics.scopePairs[scope] += int64(pairs)
	for _, event := range result.RuleEvidenceEvents {
		key := ruleMetricKey{
			scoringScope: scope,
			ruleID:       event.Payload.RuleID, ruleVersion: event.Payload.RuleVersion,
		}
		service.metrics.ruleEvidence[key]++
	}
}

func prometheusLabelValue(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	return strings.ReplaceAll(value, "\"", "\\\"")
}

func scopeLabels(scope scoringScope) string {
	return fmt.Sprintf(
		"tenant_id=\"%s\",product_id=\"%s\",model_name=\"%s\",model_run_id=\"%s\",rule_rollout_id=\"%s\"",
		prometheusLabelValue(scope.tenantID), prometheusLabelValue(scope.productID),
		prometheusLabelValue(scope.modelName), prometheusLabelValue(scope.modelRun),
		prometheusLabelValue(scope.rolloutID),
	)
}

func (service *HTTPService) prometheusMetrics(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
	values := []struct {
		name  string
		help  string
		kind  string
		value int64
	}{
		{"risk_scorer_requests_total", "HTTP scoring requests received.", "counter", service.metrics.requests.Load()},
		{"risk_scorer_errors_total", "HTTP scoring requests that failed.", "counter", service.metrics.errors.Load()},
		{"risk_scorer_hands_scored_total", "Complete hands scored successfully.", "counter", service.metrics.handsScored.Load()},
		{"risk_scorer_pairs_scored_total", "Pair rows scored successfully.", "counter", service.metrics.pairsScored.Load()},
		{"risk_scorer_inference_latency_microseconds_total", "Cumulative complete-hand scoring latency.", "counter", service.metrics.latencyMicros.Load()},
		{"risk_scorer_ready_failures_total", "Readiness checks that failed.", "counter", service.metrics.readyFailures.Load()},
		{"risk_scorer_inflight_requests", "HTTP scoring requests currently in flight.", "gauge", service.metrics.inflight.Load()},
		{"risk_scorer_last_success_unixtime", "Unix timestamp of the last successful hand score.", "gauge", service.metrics.lastSuccessUnix.Load()},
	}
	for _, metric := range values {
		fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s %s\n%s %s\n", metric.name, metric.help, metric.name, metric.kind, metric.name, strconv.FormatInt(metric.value, 10))
	}
	fmt.Fprint(writer, "# HELP risk_scorer_request_duration_seconds End-to-end HTTP scoring request duration.\n# TYPE risk_scorer_request_duration_seconds histogram\n")
	for index, upper := range requestLatencyUpperMicros {
		fmt.Fprintf(writer, "risk_scorer_request_duration_seconds_bucket{le=\"%g\"} %d\n", float64(upper)/1e6, service.metrics.requestBuckets[index].Load())
	}
	count := service.metrics.requestCount.Load()
	fmt.Fprintf(writer, "risk_scorer_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", count)
	fmt.Fprintf(writer, "risk_scorer_request_duration_seconds_sum %.9f\n", float64(service.metrics.requestMicros.Load())/1e6)
	fmt.Fprintf(writer, "risk_scorer_request_duration_seconds_count %d\n", count)

	service.metrics.dynamicMu.Lock()
	scopes := make([]scoringScope, 0, len(service.metrics.scopeHands))
	for scope := range service.metrics.scopeHands {
		scopes = append(scopes, scope)
	}
	sort.Slice(scopes, func(left, right int) bool {
		return scopeLabels(scopes[left]) < scopeLabels(scopes[right])
	})
	rules := make([]ruleMetricKey, 0, len(service.metrics.ruleEvidence))
	for key := range service.metrics.ruleEvidence {
		rules = append(rules, key)
	}
	sort.Slice(rules, func(left, right int) bool {
		leftKey := scopeLabels(rules[left].scoringScope) + rules[left].ruleID + strconv.Itoa(rules[left].ruleVersion)
		rightKey := scopeLabels(rules[right].scoringScope) + rules[right].ruleID + strconv.Itoa(rules[right].ruleVersion)
		return leftKey < rightKey
	})
	scopeHands := make(map[scoringScope]int64, len(service.metrics.scopeHands))
	scopePairs := make(map[scoringScope]int64, len(service.metrics.scopePairs))
	ruleEvidence := make(map[ruleMetricKey]int64, len(service.metrics.ruleEvidence))
	for key, value := range service.metrics.scopeHands {
		scopeHands[key] = value
	}
	for key, value := range service.metrics.scopePairs {
		scopePairs[key] = value
	}
	for key, value := range service.metrics.ruleEvidence {
		ruleEvidence[key] = value
	}
	service.metrics.dynamicMu.Unlock()

	fmt.Fprint(writer, "# HELP risk_scorer_scope_hands_scored_total Complete hands scored by governed tenant/model/rollout scope.\n# TYPE risk_scorer_scope_hands_scored_total counter\n")
	fmt.Fprint(writer, "# HELP risk_scorer_scope_pairs_scored_total Pair rows scored by governed tenant/model/rollout scope.\n# TYPE risk_scorer_scope_pairs_scored_total counter\n")
	for _, scope := range scopes {
		labels := scopeLabels(scope)
		fmt.Fprintf(writer, "risk_scorer_scope_hands_scored_total{%s} %d\n", labels, scopeHands[scope])
		fmt.Fprintf(writer, "risk_scorer_scope_pairs_scored_total{%s} %d\n", labels, scopePairs[scope])
	}
	fmt.Fprint(writer, "# HELP risk_scorer_rule_evidence_total Fired rule-evidence events by exact governed lineage.\n# TYPE risk_scorer_rule_evidence_total counter\n")
	for _, key := range rules {
		fmt.Fprintf(
			writer,
			"risk_scorer_rule_evidence_total{%s,rule_id=\"%s\",rule_version=\"%d\"} %d\n",
			scopeLabels(key.scoringScope), prometheusLabelValue(key.ruleID),
			key.ruleVersion, ruleEvidence[key],
		)
	}
	rolloutID, entries := service.scorer.RuleRolloutSnapshot()
	sort.Slice(entries, func(left, right int) bool { return entries[left].RuleID < entries[right].RuleID })
	fmt.Fprint(writer, "# HELP risk_scorer_rule_enabled Governed rule rollout enablement (1 enabled, 0 disabled).\n# TYPE risk_scorer_rule_enabled gauge\n")
	for _, entry := range entries {
		value := 0
		if entry.Enabled {
			value = 1
		}
		fmt.Fprintf(
			writer,
			"risk_scorer_rule_enabled{model_name=\"%s\",model_run_id=\"%s\",rule_rollout_id=\"%s\",rule_id=\"%s\",rule_version=\"%d\",runtime=\"%s\"} %d\n",
			prometheusLabelValue(service.scorer.bundle.Contract.ModelName),
			prometheusLabelValue(service.scorer.bundle.Contract.RunID),
			prometheusLabelValue(rolloutID), prometheusLabelValue(entry.RuleID),
			entry.RuleVersion, prometheusLabelValue(entry.Runtime), value,
		)
	}
}

func decodeJSONRequest(writer http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, 8<<20)
	decoder := json.NewDecoder(request.Body)
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("request must contain exactly one JSON value")
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
