package handlers

import (
	"testing"
	"time"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
)

func TestInvokeLifecycleBeginInvocationTracksState(t *testing.T) {
	cfg := autoscaler.Config{Platform: "faasd", Enabled: true, DefaultIdleDuration: time.Minute, CheckInterval: time.Hour}
	controller := NewFaasdAutoScaler(nil, nil, NewInMemoryFunctionStore(), "/var/openfaas/secrets", false, cfg)
	controller.RegisterFunctionWithState("openfaas-fn", "echo", map[string]string{"com.openfaas.scale.zero": "true"}, autoscaler.StateActive)

	lifecycle := NewInvokeLifecycle(controller)
	err := lifecycle.StartInvocation(nil, "echo.openfaas-fn")
	if err != nil {
		t.Fatalf("expected begin invocation to succeed, got %v", err)
	}

	key := controller.scaleKey("openfaas-fn", "echo")
	state, ok := controller.autoScaler.GetState(key)
	if !ok {
		t.Fatal("expected function state")
	}
	if state != autoscaler.StateBlocked {
		t.Fatalf("expected blocked state, got %s", state)
	}

	lifecycle.EndInvocation(nil, "echo.openfaas-fn")
	state, ok = controller.autoScaler.GetState(key)
	if !ok {
		t.Fatal("expected function state after done")
	}
	if state != autoscaler.StateActive {
		t.Fatalf("expected active state after done, got %s", state)
	}
}

func TestInvokeLifecycleBeginInvocationDisabledNoop(t *testing.T) {
	cfg := autoscaler.Config{Platform: "faasd", Enabled: false, DefaultIdleDuration: time.Minute, CheckInterval: time.Hour}
	controller := NewFaasdAutoScaler(nil, nil, NewInMemoryFunctionStore(), "/var/openfaas/secrets", false, cfg)

	lifecycle := NewInvokeLifecycle(controller)
	err := lifecycle.StartInvocation(nil, "echo")
	if err != nil {
		t.Fatalf("expected no error for disabled autoscaler, got %v", err)
	}
	lifecycle.EndInvocation(nil, "echo")
}
