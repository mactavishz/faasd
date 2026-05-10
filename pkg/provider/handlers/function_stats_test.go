package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

func TestFunctionStatsStoreRecordAndReset(t *testing.T) {
	store := &functionStatsStore{stats: map[string]FunctionStats{}}
	record := InvocationRecord{StatusCode: http.StatusOK, Success: true}
	store.Record("openfaas-fn", "echo", record)

	stats, ok := store.Get("openfaas-fn", "echo")
	if !ok {
		t.Fatal("expected stats to exist")
	}
	if stats.Summary.SuccessfulInvocations != 1 {
		t.Fatalf("expected 1 success, got %d", stats.Summary.SuccessfulInvocations)
	}
	if stats.Summary.StatusCodes["200"] != 1 {
		t.Fatalf("expected 200 count 1, got %d", stats.Summary.StatusCodes["200"])
	}

	store.Reset("openfaas-fn", "echo")
	if _, ok := store.Get("openfaas-fn", "echo"); ok {
		t.Fatal("expected stats to be reset")
	}
}

func TestFunctionStatsMiddlewareRecordsInvocation(t *testing.T) {
	faasdFunctionStats = &functionStatsStore{stats: map[string]FunctionStats{}}
	req := httptest.NewRequest(http.MethodPost, "/function/echo", nil)
	req = mux.SetURLVars(req, map[string]string{"name": "echo"})
	rec := httptest.NewRecorder()

	handler := MakeFunctionStatsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	handler(rec, req)

	stats, ok := faasdFunctionStats.Get("openfaas-fn", "echo")
	if !ok {
		t.Fatal("expected recorded stats")
	}
	if len(stats.Invocations) != 1 {
		t.Fatalf("expected 1 invocation, got %d", len(stats.Invocations))
	}
	if stats.Invocations[0].StatusCode != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", stats.Invocations[0].StatusCode)
	}
	if !stats.Invocations[0].FinishedAt.After(stats.Invocations[0].StartedAt) && !stats.Invocations[0].FinishedAt.Equal(stats.Invocations[0].StartedAt) {
		t.Fatal("expected finished time to be at or after start")
	}
	if stats.Summary.StatusCodes["202"] != 1 {
		t.Fatalf("expected 202 count 1, got %d", stats.Summary.StatusCodes["202"])
	}
}

func TestFunctionStatsResponseJSONShape(t *testing.T) {
	resp := FunctionStatsResponse{}
	resp.Function.Name = "echo"
	resp.Function.Namespace = "openfaas-fn"
	resp.Summary.StatusCodes = map[string]int{"200": 1}
	resp.Invocations = []InvocationRecord{{
		StartedAt:  time.Date(2026, 5, 7, 10, 1, 0, 0, time.UTC),
		FinishedAt: time.Date(2026, 5, 7, 10, 1, 0, 2500000, time.UTC),
		DurationNS: 2500000,
		Method:     http.MethodPost,
		Path:       "/fn/echo",
		StatusCode: 200,
		Success:    true,
	}}

	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("expected non-empty json body")
	}
}
