package handlers

import (
	"testing"
	"time"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"github.com/openfaas/faas-provider/types"
)

func TestSetFunctionStore_NilResetsToInMemoryStore(t *testing.T) {
	SetFunctionStore(nil)

	stored := PutFunctionFromDeployment(types.FunctionDeployment{Service: "fn", Image: "repo/fn:latest"}, "openfaas-fn")
	if stored.Name != "fn" {
		t.Fatalf("expected stored function name fn, got %q", stored.Name)
	}

	got, ok := GetStoredFunction("openfaas-fn", "fn")
	if !ok {
		t.Fatal("expected function in active store")
	}
	if got.Image != "repo/fn:latest" {
		t.Fatalf("expected stored image repo/fn:latest, got %q", got.Image)
	}

	DeleteStoredFunction("openfaas-fn", "fn")
	if _, ok := GetStoredFunction("openfaas-fn", "fn"); ok {
		t.Fatal("expected function to be deleted from active store")
	}
}

func TestSetFunctionStore_CustomStoreBacksHelpers(t *testing.T) {
	store := NewInMemoryFunctionStore()
	SetFunctionStore(store)
	t.Cleanup(func() { SetFunctionStore(nil) })

	PutFunctionFromDeployment(types.FunctionDeployment{Service: "a", Image: "repo/a:1"}, "openfaas-fn")
	PutFunctionFromDeployment(types.FunctionDeployment{Service: "b", Image: "repo/b:1"}, "openfaas-fn")

	list := ListStoredFunctions("openfaas-fn")
	if len(list) != 2 {
		t.Fatalf("expected 2 functions in store, got %d", len(list))
	}

	if _, ok := store.Get("openfaas-fn", "a"); !ok {
		t.Fatal("expected helper writes to custom store")
	}

	DeleteStoredFunction("openfaas-fn", "a")
	if _, ok := store.Get("openfaas-fn", "a"); ok {
		t.Fatal("expected helper delete to affect custom store")
	}
}

func TestSetAutoScalerController_SetAndClear(t *testing.T) {
	SetAutoScalerController(nil)
	if got := getAutoScalerController(); got != nil {
		t.Fatalf("expected nil controller, got %#v", got)
	}

	cfg := autoscaler.Config{Platform: "faasd", Enabled: true, DefaultIdleDuration: time.Minute, CheckInterval: time.Hour}
	controller := NewFaasdAutoScalerController(nil, nil, NewInMemoryFunctionStore(), "/var/openfaas/secrets", false, cfg)
	SetAutoScalerController(controller)

	if got := getAutoScalerController(); got != controller {
		t.Fatal("expected controller to be set")
	}

	SetAutoScalerController(nil)
	if got := getAutoScalerController(); got != nil {
		t.Fatalf("expected nil controller after clear, got %#v", got)
	}
}

func TestNewFaasdAutoScaler_EnabledReflectsConfig(t *testing.T) {
	disabled := NewFaasdAutoScalerController(nil, nil, NewInMemoryFunctionStore(), "/var/openfaas/secrets", false, autoscaler.Config{Platform: "faasd", Enabled: false, DefaultIdleDuration: time.Minute, CheckInterval: time.Hour})
	if disabled.Enabled() {
		t.Fatal("expected disabled autoscaler")
	}

	enabled := NewFaasdAutoScalerController(nil, nil, NewInMemoryFunctionStore(), "/var/openfaas/secrets", false, autoscaler.Config{Platform: "faasd", Enabled: true, DefaultIdleDuration: time.Minute, CheckInterval: time.Hour})
	if !enabled.Enabled() {
		t.Fatal("expected enabled autoscaler")
	}
}

func TestFaasdAutoScaler_StartStopNilSafe(t *testing.T) {
	var nilController *FaasdAutoScalerController
	nilController.Start()
	nilController.Stop()

	cfg := autoscaler.Config{Platform: "faasd", Enabled: false, DefaultIdleDuration: time.Minute, CheckInterval: time.Hour}
	controller := NewFaasdAutoScalerController(nil, nil, NewInMemoryFunctionStore(), "/var/openfaas/secrets", false, cfg)
	controller.Start()
	controller.Stop()
}

func TestFaasdAutoScaler_DisabledMethodsAreNoop(t *testing.T) {
	cfg := autoscaler.Config{Platform: "faasd", Enabled: false, DefaultIdleDuration: time.Minute, CheckInterval: time.Hour}
	controller := NewFaasdAutoScalerController(nil, nil, NewInMemoryFunctionStore(), "/var/openfaas/secrets", false, cfg)

	controller.RegisterFunctionWithState("openfaas-fn", "fn", map[string]string{"k": "v"}, autoscaler.StateScaledDown)
	controller.RecordActivity("openfaas-fn", "fn")
	controller.UnregisterFunction("openfaas-fn", "fn")

	if statuses := controller.autoScaler.GetFunctionStatus(); len(statuses) != 0 {
		t.Fatalf("expected disabled autoscaler to track no functions, got %d", len(statuses))
	}
}

func TestFaasdAutoScaler_EnabledMethodsDelegateToAutoscaler(t *testing.T) {
	cfg := autoscaler.Config{Platform: "faasd", Enabled: true, DefaultIdleDuration: time.Minute, CheckInterval: time.Hour}
	controller := NewFaasdAutoScalerController(nil, nil, NewInMemoryFunctionStore(), "/var/openfaas/secrets", false, cfg)

	controller.RegisterFunctionWithState("openfaas-fn", "fn", map[string]string{"com.openfaas.scale.zero": "true"}, autoscaler.StateScaledDown)
	key := controller.scaleKey("openfaas-fn", "fn")

	statuses := controller.autoScaler.GetFunctionStatus()
	if _, ok := statuses[key]; !ok {
		t.Fatalf("expected registered function key %q in autoscaler status", key)
	}

	state, ok := controller.autoScaler.GetState(key)
	if !ok {
		t.Fatalf("expected state for %q", key)
	}
	if state != autoscaler.StateScaledDown {
		t.Fatalf("expected scaled-down state, got %s", state)
	}

	beforeStatus := controller.autoScaler.GetFunctionStatus()[key]
	time.Sleep(2 * time.Millisecond)
	controller.RecordActivity("openfaas-fn", "fn")
	afterStatus := controller.autoScaler.GetFunctionStatus()[key]
	if !afterStatus.LastAccessTime.After(beforeStatus.LastAccessTime) {
		t.Fatalf("expected LastAccessTime to move forward, before=%v after=%v", beforeStatus.LastAccessTime, afterStatus.LastAccessTime)
	}

	controller.UnregisterFunction("openfaas-fn", "fn")
	statuses = controller.autoScaler.GetFunctionStatus()
	if _, ok := statuses[key]; ok {
		t.Fatalf("expected function key %q to be unregistered", key)
	}
}

func TestScaleKeyAndParseScaleKey(t *testing.T) {
	controller := &FaasdAutoScalerController{}

	if got := controller.scaleKey("ns", "fn"); got != "ns/fn" {
		t.Fatalf("unexpected scale key: %q", got)
	}

	ns, name := controller.parseScaleKey("ns/fn")
	if ns != "ns" || name != "fn" {
		t.Fatalf("unexpected parsed key, ns=%q name=%q", ns, name)
	}

	ns, name = controller.parseScaleKey("fn")
	if ns != "" || name != "fn" {
		t.Fatalf("unexpected parsed single-part key, ns=%q name=%q", ns, name)
	}
}

func TestEnsureFunctionLabelsForAutoscaler_ClonesMap(t *testing.T) {
	labels := map[string]string{"k": "v"}
	out := ensureFunctionLabelsForAutoscaler(labels)
	out["k"] = "changed"

	if labels["k"] != "v" {
		t.Fatalf("expected input labels to remain unchanged, got %q", labels["k"])
	}
}

func TestEnsureFunctionLabelsForAutoscaler_NilInputReturnsEmptyMap(t *testing.T) {
	out := ensureFunctionLabelsForAutoscaler(nil)
	if out == nil {
		t.Fatal("expected non-nil map")
	}
	out["k"] = "v"
	if len(out) != 1 {
		t.Fatalf("expected map to be writable, got len=%d", len(out))
	}
}

func TestStartEndInvocationTransitionsToBlockedAndBack(t *testing.T) {
	cfg := autoscaler.Config{Platform: "faasd", Enabled: true, DefaultIdleDuration: time.Minute, CheckInterval: time.Hour}
	controller := NewFaasdAutoScalerController(nil, nil, NewInMemoryFunctionStore(), "/var/openfaas/secrets", false, cfg)
	controller.RegisterFunctionWithState("openfaas-fn", "fn", map[string]string{"com.openfaas.scale.zero": "true"}, autoscaler.StateActive)

	err := controller.StartInvocation("openfaas-fn", "fn")
	if err != nil {
		t.Fatalf("expected start invocation to succeed, got %v", err)
	}

	key := controller.scaleKey("openfaas-fn", "fn")
	state, ok := controller.autoScaler.GetState(key)
	if !ok {
		t.Fatal("expected function state")
	}
	if state != autoscaler.StateBlocked {
		t.Fatalf("expected blocked state, got %s", state)
	}

	controller.EndInvocation("openfaas-fn", "fn")
	state, ok = controller.autoScaler.GetState(key)
	if !ok {
		t.Fatal("expected function state after end")
	}
	if state != autoscaler.StateActive {
		t.Fatalf("expected active state, got %s", state)
	}
}
