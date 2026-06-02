package handlers

import (
	"sort"
	"sync"
	"time"

	"github.com/openfaas/faas-provider/types"
	"k8s.io/apimachinery/pkg/api/resource"
)

type StoredFunction struct {
	Name            string
	Namespace       string
	Image           string
	Labels          map[string]string
	Annotations     map[string]string
	Secrets         []string
	EnvVars         map[string]string
	EnvProcess      string
	Limits          *types.FunctionResources
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DesiredReplicas uint64
	ArchiveBacked   bool
}

type FunctionStore interface {
	Get(namespace, name string) (StoredFunction, bool)
	Put(function StoredFunction)
	Delete(namespace, name string)
	List(namespace string) []StoredFunction
}

type InMemoryFunctionStore struct {
	mu        sync.RWMutex
	functions map[string]StoredFunction
}

func NewInMemoryFunctionStore() *InMemoryFunctionStore {
	return &InMemoryFunctionStore{functions: make(map[string]StoredFunction)}
}

func (s *InMemoryFunctionStore) Get(namespace, name string) (StoredFunction, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fn, ok := s.functions[storeKey(namespace, name)]
	if !ok {
		return StoredFunction{}, false
	}

	return cloneStoredFunction(fn), true
}

func (s *InMemoryFunctionStore) Put(function StoredFunction) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := storeKey(function.Namespace, function.Name)
	now := time.Now()
	if existing, ok := s.functions[key]; ok {
		function.CreatedAt = existing.CreatedAt
	} else if function.CreatedAt.IsZero() {
		function.CreatedAt = now
	}

	function.UpdatedAt = now
	if function.DesiredReplicas == 0 {
		function.DesiredReplicas = 1
	}

	s.functions[key] = cloneStoredFunction(function)
}

func (s *InMemoryFunctionStore) Delete(namespace, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.functions, storeKey(namespace, name))
}

func (s *InMemoryFunctionStore) List(namespace string) []StoredFunction {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]StoredFunction, 0, len(s.functions))
	for _, fn := range s.functions {
		if fn.Namespace == namespace {
			result = append(result, cloneStoredFunction(fn))
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

func NewStoredFunctionFromDeployment(req types.FunctionDeployment, namespace string) StoredFunction {
	return NewStoredFunctionFromDeploymentWithSource(req, namespace, false)
}

func NewStoredFunctionFromDeploymentWithSource(req types.FunctionDeployment, namespace string, archiveBacked bool) StoredFunction {
	labels := map[string]string{}
	if req.Labels != nil {
		for k, v := range *req.Labels {
			labels[k] = v
		}
	}

	annotations := map[string]string{}
	if req.Annotations != nil {
		for k, v := range *req.Annotations {
			annotations[k] = v
		}
	}

	secrets := make([]string, len(req.Secrets))
	copy(secrets, req.Secrets)

	envVars := map[string]string{}
	for k, v := range req.EnvVars {
		envVars[k] = v
	}

	var limits *types.FunctionResources
	if req.Limits != nil {
		limits = &types.FunctionResources{Memory: req.Limits.Memory, CPU: req.Limits.CPU}
	}

	return StoredFunction{
		Name:            req.Service,
		Namespace:       namespace,
		Image:           req.Image,
		Labels:          labels,
		Annotations:     annotations,
		Secrets:         secrets,
		EnvVars:         envVars,
		EnvProcess:      req.EnvProcess,
		Limits:          limits,
		DesiredReplicas: 1,
		ArchiveBacked:   archiveBacked,
	}
}

func NewStoredFunctionFromRuntime(fn Function) StoredFunction {
	labels := map[string]string{}
	for k, v := range fn.labels {
		labels[k] = v
	}

	annotations := map[string]string{}
	for k, v := range fn.annotations {
		annotations[k] = v
	}

	secrets := make([]string, len(fn.secrets))
	copy(secrets, fn.secrets)

	envVars := map[string]string{}
	for k, v := range fn.envVars {
		envVars[k] = v
	}

	limits := &types.FunctionResources{}
	if fn.memoryLimit > 0 {
		limits.Memory = resource.NewQuantity(fn.memoryLimit, resource.BinarySI).String()
	}
	if fn.cpuLimit > 0 {
		limits.CPU = resource.NewScaledQuantity(fn.cpuLimit, resource.Nano).String()
	}
	if limits.Memory == "" && limits.CPU == "" {
		limits = nil
	}

	return StoredFunction{
		Name:            fn.name,
		Namespace:       fn.namespace,
		Image:           fn.image,
		Labels:          labels,
		Annotations:     annotations,
		Secrets:         secrets,
		EnvVars:         envVars,
		EnvProcess:      fn.envProcess,
		Limits:          limits,
		CreatedAt:       fn.createdAt,
		DesiredReplicas: 1,
	}
}

func (f StoredFunction) ToDeployment() types.FunctionDeployment {
	labels := map[string]string{}
	for k, v := range f.Labels {
		labels[k] = v
	}

	annotations := map[string]string{}
	for k, v := range f.Annotations {
		annotations[k] = v
	}

	envVars := map[string]string{}
	for k, v := range f.EnvVars {
		envVars[k] = v
	}

	secrets := make([]string, len(f.Secrets))
	copy(secrets, f.Secrets)

	req := types.FunctionDeployment{
		Service:    f.Name,
		Image:      f.Image,
		EnvProcess: f.EnvProcess,
		EnvVars:    envVars,
		Secrets:    secrets,
		Namespace:  f.Namespace,
	}

	if len(labels) > 0 {
		req.Labels = &labels
	}
	if len(annotations) > 0 {
		req.Annotations = &annotations
	}
	if f.Limits != nil {
		req.Limits = &types.FunctionResources{Memory: f.Limits.Memory, CPU: f.Limits.CPU}
	}

	return req
}

func storeKey(namespace, name string) string {
	return namespace + "/" + name
}

func cloneStoredFunction(in StoredFunction) StoredFunction {
	labels := map[string]string{}
	for k, v := range in.Labels {
		labels[k] = v
	}

	annotations := map[string]string{}
	for k, v := range in.Annotations {
		annotations[k] = v
	}

	envVars := map[string]string{}
	for k, v := range in.EnvVars {
		envVars[k] = v
	}

	secrets := make([]string, len(in.Secrets))
	copy(secrets, in.Secrets)

	var limits *types.FunctionResources
	if in.Limits != nil {
		limits = &types.FunctionResources{Memory: in.Limits.Memory, CPU: in.Limits.CPU}
	}

	out := in
	out.Labels = labels
	out.Annotations = annotations
	out.EnvVars = envVars
	out.Secrets = secrets
	out.Limits = limits

	return out
}
