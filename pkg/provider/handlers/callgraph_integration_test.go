package handlers

import (
	"net/http"
	"testing"
	"time"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
)

func TestExtractCaller_RightmostFromHeader(t *testing.T) {
	cfg := *callgraph.DefaultConfig()
	controller := NewFaasdCallGraphController(nil, nil, NewInMemoryFunctionStore(), cfg, nil)

	controller.upsertFunction("openfaas-fn", "tree-a", nil, "10.62.0.40", true)
	controller.upsertFunction("openfaas-fn", "tree-b", nil, "10.62.0.41", true)

	req, err := http.NewRequest(http.MethodPost, "http://provider/function/tree-b", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Forwarded-For", "203.0.113.10, 10.62.0.40:41231, 10.62.0.41:32218")

	caller, found, enabled := controller.extractCaller(req)
	if !found {
		t.Fatal("expected caller to be found from forwarded chain")
	}
	if caller != "tree-b" {
		t.Fatalf("expected rightmost known caller tree-b, got %q", caller)
	}
	if !enabled {
		t.Fatal("expected caller to be callgraph-enabled")
	}
}

func TestExtractCaller_NotFound(t *testing.T) {
	cfg := *callgraph.DefaultConfig()
	controller := NewFaasdCallGraphController(nil, nil, NewInMemoryFunctionStore(), cfg, nil)

	controller.upsertFunction("openfaas-fn", "tree-a", nil, "10.62.0.40", true)

	req, err := http.NewRequest(http.MethodPost, "http://provider/function/tree-b", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Forwarded-For", "198.51.100.8, 198.51.100.9")

	caller, found, enabled := controller.extractCaller(req)
	if found {
		t.Fatalf("expected no caller match, got %q", caller)
	}
	if enabled {
		t.Fatal("expected caller-enabled to be false when caller not found")
	}
}

func TestExtractCaller_ExecutionContextFallback(t *testing.T) {
	cfg := *callgraph.DefaultConfig()
	controller := NewFaasdCallGraphController(nil, nil, NewInMemoryFunctionStore(), cfg, nil)

	controller.upsertFunction("openfaas-fn", "tree-c", nil, "10.62.0.41", true)
	controller.callGraphTracker.StartExecution("tree-c", "req-async", "exec-c", time.Now())

	req, err := http.NewRequest(http.MethodPost, "http://provider/function/tree-f", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Call-Id", "req-async")
	req.Header.Set("X-Exec-Id", "exec-c")
	req.Header.Set("X-Forwarded-For", "203.0.113.10")

	caller, found, enabled := controller.extractCaller(req)
	if !found {
		t.Fatal("expected caller to be recovered from execution context")
	}
	if caller != "tree-c" {
		t.Fatalf("expected async caller tree-c, got %q", caller)
	}
	if !enabled {
		t.Fatal("expected async caller to be callgraph-enabled")
	}
}
