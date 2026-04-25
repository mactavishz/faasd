package handlers

import (
	"testing"
	"time"

	"github.com/openfaas/faas-provider/types"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestInMemoryFunctionStore_PutGetDeleteAndCopies(t *testing.T) {
	store := NewInMemoryFunctionStore()

	store.Put(StoredFunction{
		Name:        "fn-a",
		Namespace:   "openfaas-fn",
		Image:       "repo/fn-a:latest",
		Labels:      map[string]string{"k": "v"},
		Annotations: map[string]string{"a": "b"},
		Secrets:     []string{"s1"},
		EnvVars:     map[string]string{"E": "1"},
	})

	stored, ok := store.Get("openfaas-fn", "fn-a")
	if !ok {
		t.Fatal("expected function to exist")
	}

	if stored.DesiredReplicas != 1 {
		t.Fatalf("expected DesiredReplicas to default to 1, got %d", stored.DesiredReplicas)
	}
	if stored.CreatedAt.IsZero() {
		t.Fatal("expected CreatedAt to be set")
	}
	if stored.UpdatedAt.IsZero() {
		t.Fatal("expected UpdatedAt to be set")
	}

	stored.Labels["k"] = "changed"
	stored.Annotations["a"] = "changed"
	stored.Secrets[0] = "changed"
	stored.EnvVars["E"] = "changed"

	again, ok := store.Get("openfaas-fn", "fn-a")
	if !ok {
		t.Fatal("expected function to exist")
	}

	if again.Labels["k"] != "v" {
		t.Fatalf("expected labels to be copied, got %q", again.Labels["k"])
	}
	if again.Annotations["a"] != "b" {
		t.Fatalf("expected annotations to be copied, got %q", again.Annotations["a"])
	}
	if again.Secrets[0] != "s1" {
		t.Fatalf("expected secrets to be copied, got %q", again.Secrets[0])
	}
	if again.EnvVars["E"] != "1" {
		t.Fatalf("expected env vars to be copied, got %q", again.EnvVars["E"])
	}

	store.Delete("openfaas-fn", "fn-a")
	if _, ok := store.Get("openfaas-fn", "fn-a"); ok {
		t.Fatal("expected function to be deleted")
	}
}

func TestInMemoryFunctionStore_PutPreservesCreatedAtOnUpdate(t *testing.T) {
	store := NewInMemoryFunctionStore()

	store.Put(StoredFunction{Name: "fn-a", Namespace: "openfaas-fn", Image: "repo/a:1"})
	first, _ := store.Get("openfaas-fn", "fn-a")

	time.Sleep(2 * time.Millisecond)
	store.Put(StoredFunction{Name: "fn-a", Namespace: "openfaas-fn", Image: "repo/a:2"})
	second, _ := store.Get("openfaas-fn", "fn-a")

	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("expected CreatedAt to be preserved, first=%v second=%v", first.CreatedAt, second.CreatedAt)
	}
	if !second.UpdatedAt.After(first.UpdatedAt) {
		t.Fatalf("expected UpdatedAt to move forward, first=%v second=%v", first.UpdatedAt, second.UpdatedAt)
	}
	if second.Image != "repo/a:2" {
		t.Fatalf("expected latest value to be stored, got %q", second.Image)
	}
}

func TestInMemoryFunctionStore_ListFiltersAndSortsAndCopies(t *testing.T) {
	store := NewInMemoryFunctionStore()
	store.Put(StoredFunction{Name: "z", Namespace: "openfaas-fn", Labels: map[string]string{"a": "1"}})
	store.Put(StoredFunction{Name: "a", Namespace: "openfaas-fn", Labels: map[string]string{"a": "2"}})
	store.Put(StoredFunction{Name: "ignored", Namespace: "other"})

	list := store.List("openfaas-fn")
	if len(list) != 2 {
		t.Fatalf("expected 2 items in namespace, got %d", len(list))
	}
	if list[0].Name != "a" || list[1].Name != "z" {
		t.Fatalf("expected sorted names [a z], got [%s %s]", list[0].Name, list[1].Name)
	}

	list[0].Labels["a"] = "changed"
	again := store.List("openfaas-fn")
	if again[0].Labels["a"] != "2" {
		t.Fatalf("expected list to return deep copy, got %q", again[0].Labels["a"])
	}
}

func TestNewStoredFunctionFromDeployment_CopiesAllMutableData(t *testing.T) {
	labels := map[string]string{"l": "1"}
	annotations := map[string]string{"a": "1"}
	req := types.FunctionDeployment{
		Service:     "fn",
		Image:       "repo/fn:latest",
		EnvProcess:  "python index.py",
		Labels:      &labels,
		Annotations: &annotations,
		Secrets:     []string{"s1"},
		EnvVars:     map[string]string{"E": "1"},
		Limits:      &types.FunctionResources{Memory: "128Mi", CPU: "200m"},
	}

	stored := NewStoredFunctionFromDeployment(req, "openfaas-fn")

	if stored.Name != "fn" || stored.Namespace != "openfaas-fn" || stored.Image != "repo/fn:latest" {
		t.Fatalf("unexpected basic fields: %#v", stored)
	}
	if stored.DesiredReplicas != 1 {
		t.Fatalf("expected DesiredReplicas=1, got %d", stored.DesiredReplicas)
	}
	if stored.Limits == nil || stored.Limits.Memory != "128Mi" || stored.Limits.CPU != "200m" {
		t.Fatalf("unexpected limits: %#v", stored.Limits)
	}

	labels["l"] = "changed"
	annotations["a"] = "changed"
	req.Secrets[0] = "changed"
	req.EnvVars["E"] = "changed"
	req.Limits.Memory = "256Mi"

	if stored.Labels["l"] != "1" || stored.Annotations["a"] != "1" {
		t.Fatalf("expected maps to be copied, got labels=%v annotations=%v", stored.Labels, stored.Annotations)
	}
	if stored.Secrets[0] != "s1" || stored.EnvVars["E"] != "1" {
		t.Fatalf("expected slices/maps to be copied, got secrets=%v env=%v", stored.Secrets, stored.EnvVars)
	}
	if stored.Limits.Memory != "128Mi" {
		t.Fatalf("expected limits to be copied, got %#v", stored.Limits)
	}
}

func TestNewStoredFunctionFromRuntime_ConvertsLimitsAndCopiesData(t *testing.T) {
	created := time.Now().Add(-time.Minute)
	fn := Function{
		name:        "fn",
		namespace:   "openfaas-fn",
		image:       "repo/fn:latest",
		labels:      map[string]string{"l": "1"},
		annotations: map[string]string{"a": "1"},
		secrets:     []string{"s1"},
		envVars:     map[string]string{"E": "1"},
		envProcess:  "python index.py",
		memoryLimit: 256 * 1024 * 1024,
		cpuLimit:    50_000_000,
		createdAt:   created,
	}

	stored := NewStoredFunctionFromRuntime(fn)

	if stored.Name != "fn" || stored.Namespace != "openfaas-fn" || stored.Image != "repo/fn:latest" {
		t.Fatalf("unexpected basic fields: %#v", stored)
	}
	if !stored.CreatedAt.Equal(created) {
		t.Fatalf("expected CreatedAt to be copied, got %v want %v", stored.CreatedAt, created)
	}
	if stored.Limits == nil {
		t.Fatal("expected limits to be set")
	}

	memQ, err := resource.ParseQuantity(stored.Limits.Memory)
	if err != nil {
		t.Fatalf("parse memory quantity: %v", err)
	}
	if memQ.Value() != fn.memoryLimit {
		t.Fatalf("unexpected memory quantity value, got %d want %d", memQ.Value(), fn.memoryLimit)
	}

	cpuQ, err := resource.ParseQuantity(stored.Limits.CPU)
	if err != nil {
		t.Fatalf("parse cpu quantity: %v", err)
	}
	if cpuQ.MilliValue() != fn.cpuLimit/1_000_000 {
		t.Fatalf("unexpected cpu quantity milli value, got %d want %d", cpuQ.MilliValue(), fn.cpuLimit/1_000_000)
	}

	fn.labels["l"] = "changed"
	fn.annotations["a"] = "changed"
	fn.secrets[0] = "changed"
	fn.envVars["E"] = "changed"

	if stored.Labels["l"] != "1" || stored.Annotations["a"] != "1" || stored.Secrets[0] != "s1" || stored.EnvVars["E"] != "1" {
		t.Fatalf("expected runtime data to be copied, got %#v", stored)
	}
}

func TestNewStoredFunctionFromRuntime_NoLimitsWhenUnset(t *testing.T) {
	stored := NewStoredFunctionFromRuntime(Function{name: "fn", namespace: "openfaas-fn", image: "repo/fn:latest"})
	if stored.Limits != nil {
		t.Fatalf("expected limits to be nil when runtime limits are unset, got %#v", stored.Limits)
	}
}

func TestStoredFunction_ToDeployment(t *testing.T) {
	stored := StoredFunction{
		Name:        "fn",
		Namespace:   "openfaas-fn",
		Image:       "repo/fn:latest",
		EnvProcess:  "python index.py",
		Labels:      map[string]string{"l": "1"},
		Annotations: map[string]string{"a": "1"},
		Secrets:     []string{"s1"},
		EnvVars:     map[string]string{"E": "1"},
		Limits:      &types.FunctionResources{Memory: "128Mi", CPU: "100m"},
	}

	dep := stored.ToDeployment()
	if dep.Service != "fn" || dep.Namespace != "openfaas-fn" || dep.Image != "repo/fn:latest" || dep.EnvProcess != "python index.py" {
		t.Fatalf("unexpected deployment fields: %#v", dep)
	}
	if dep.Labels == nil || dep.Annotations == nil || dep.Limits == nil {
		t.Fatalf("expected labels/annotations/limits to be set, got %#v", dep)
	}

	(*dep.Labels)["l"] = "changed"
	(*dep.Annotations)["a"] = "changed"
	dep.Secrets[0] = "changed"
	dep.EnvVars["E"] = "changed"
	dep.Limits.Memory = "256Mi"

	if stored.Labels["l"] != "1" || stored.Annotations["a"] != "1" || stored.Secrets[0] != "s1" || stored.EnvVars["E"] != "1" || stored.Limits.Memory != "128Mi" {
		t.Fatalf("expected deployment conversion to deep copy fields, got stored=%#v", stored)
	}
}

func TestStoredFunction_ToDeployment_EmptyMapsOmitted(t *testing.T) {
	dep := StoredFunction{Name: "fn", Namespace: "openfaas-fn", Image: "repo/fn:latest"}.ToDeployment()
	if dep.Labels != nil {
		t.Fatalf("expected empty labels to be omitted, got %#v", dep.Labels)
	}
	if dep.Annotations != nil {
		t.Fatalf("expected empty annotations to be omitted, got %#v", dep.Annotations)
	}
}
