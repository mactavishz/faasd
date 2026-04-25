package handlers

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/namespaces"
	gocni "github.com/containerd/go-cni"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"github.com/openfaas/faas-provider/types"
	"github.com/openfaas/faasd/pkg/cninetwork"
	"github.com/openfaas/faasd/pkg/service"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

var (
	storeMu             sync.RWMutex
	activeFunctionStore FunctionStore = NewInMemoryFunctionStore()
	autoscalerMu        sync.RWMutex
	activeAutoScaler    *FaasdAutoScaler
)

type FaasdAutoScaler struct {
	client         *containerd.Client
	cni            gocni.CNI
	store          FunctionStore
	baseSecretPath string
	alwaysPull     bool
	logger         *zap.Logger
	autoScaler     *autoscaler.AutoScaler
	scaleUpGroup   singleflight.Group
}

type faasdScaleOperation struct {
	controller *FaasdAutoScaler
}

func SetFunctionStore(store FunctionStore) {
	if store == nil {
		store = NewInMemoryFunctionStore()
	}

	storeMu.Lock()
	activeFunctionStore = store
	storeMu.Unlock()
}

func getFunctionStore() FunctionStore {
	storeMu.RLock()
	defer storeMu.RUnlock()
	return activeFunctionStore
}

func SetAutoScalerController(controller *FaasdAutoScaler) {
	autoscalerMu.Lock()
	activeAutoScaler = controller
	autoscalerMu.Unlock()
}

func getAutoScalerController() *FaasdAutoScaler {
	autoscalerMu.RLock()
	defer autoscalerMu.RUnlock()
	return activeAutoScaler
}

func NewFaasdAutoScaler(client *containerd.Client, cni gocni.CNI, store FunctionStore, baseSecretPath string, alwaysPull bool, cfg autoscaler.Config) *FaasdAutoScaler {
	logger := zap.NewNop()
	controller := &FaasdAutoScaler{
		client:         client,
		cni:            cni,
		store:          store,
		baseSecretPath: baseSecretPath,
		alwaysPull:     alwaysPull,
		logger:         logger,
	}

	controller.autoScaler = autoscaler.New(cfg, &faasdScaleOperation{controller: controller}, logger)
	return controller
}

func (f *FaasdAutoScaler) Start() {
	if f == nil || f.autoScaler == nil {
		return
	}
	f.autoScaler.Start()
}

func (f *FaasdAutoScaler) Stop() {
	if f == nil || f.autoScaler == nil {
		return
	}
	f.autoScaler.Stop()
}

func (f *FaasdAutoScaler) Enabled() bool {
	if f == nil || f.autoScaler == nil {
		return false
	}
	return f.autoScaler.IsEnabled()
}

func (f *FaasdAutoScaler) RegisterFunction(namespace, name string, labels map[string]string) {
	if !f.Enabled() {
		return
	}
	f.autoScaler.RegisterFunction(f.scaleKey(namespace, name), labels)
}

func (f *FaasdAutoScaler) MarkScaledDown(namespace, name string, scaledDown bool) {
	if !f.Enabled() {
		return
	}
	f.autoScaler.MarkScaledDown(f.scaleKey(namespace, name), scaledDown)
}

func (f *FaasdAutoScaler) UnregisterFunction(namespace, name string) {
	if !f.Enabled() {
		return
	}
	f.autoScaler.UnregisterFunction(f.scaleKey(namespace, name))
}

func (f *FaasdAutoScaler) RecordActivity(namespace, name string) {
	if !f.Enabled() {
		return
	}
	f.autoScaler.RecordActivity(f.scaleKey(namespace, name))
}

func (f *FaasdAutoScaler) ScaleUp(namespace, name string) error {
	_, err, _ := f.scaleUpGroup.Do(f.scaleKey(namespace, name), func() (any, error) {
		return nil, f.scaleUpLocked(namespace, name)
	})
	return err
}

func (f *FaasdAutoScaler) ScaleDown(namespace, name string) error {
	if f == nil {
		return fmt.Errorf("autoscaler controller not configured")
	}

	ctx := namespaces.WithNamespace(context.Background(), namespace)
	if function, err := GetFunction(f.client, name, namespace); err == nil {
		if function.replicas != 0 {
			if err := cninetwork.DeleteCNINetwork(ctx, f.cni, f.client, name); err != nil {
				log.Printf("[Scale] error removing CNI network for %s.%s: %s", name, namespace, err)
			}
		}
	}

	if err := service.Remove(ctx, f.client, name); err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil
		}
		return err
	}

	if f.Enabled() {
		f.autoScaler.MarkScaledDown(f.scaleKey(namespace, name), true)
	}

	return nil
}

func (f *FaasdAutoScaler) scaleUpLocked(namespace, name string) error {
	if f == nil {
		return fmt.Errorf("autoscaler controller not configured")
	}

	stored, ok := f.store.Get(namespace, name)
	if !ok {
		return fmt.Errorf("function %s.%s not found in function store", name, namespace)
	}

	ctx := namespaces.WithNamespace(context.Background(), namespace)
	if err := service.Remove(ctx, f.client, name); err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return err
		}
	}

	secretMountPath := getNamespaceSecretMountPath(f.baseSecretPath, namespace)
	req := stored.ToDeployment()
	if err := deploy(ctx, req, f.client, f.cni, secretMountPath, f.alwaysPull); err != nil {
		return err
	}

	if f.Enabled() {
		key := f.scaleKey(namespace, name)
		f.autoScaler.MarkScaledDown(key, false)
		f.autoScaler.RecordActivity(key)
	}

	return nil
}

func (f *FaasdAutoScaler) scaleKey(namespace, name string) string {
	return namespace + "/" + name
}

func (f *FaasdAutoScaler) parseScaleKey(key string) (string, string) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return "", key
	}
	return parts[0], parts[1]
}

func (f *faasdScaleOperation) ScaleDown(functionKey string) error {
	namespace, name := f.controller.parseScaleKey(functionKey)
	if namespace == "" {
		namespace = getRequestNamespace("")
	}
	return f.controller.ScaleDown(namespace, name)
}

func (f *faasdScaleOperation) ScaleUp(functionKey string) error {
	namespace, name := f.controller.parseScaleKey(functionKey)
	if namespace == "" {
		namespace = getRequestNamespace("")
	}
	return f.controller.ScaleUp(namespace, name)
}

func BootstrapFunctionStore(client *containerd.Client, store FunctionStore, namespace string) {
	if store == nil {
		return
	}

	functions, err := ListFunctions(client, namespace)
	if err != nil {
		return
	}

	for _, function := range functions {
		store.Put(NewStoredFunctionFromRuntime(*function))
	}
}

// BuildFunctionStatus is the shared status builder used by read/replica handlers.
// It combines desired state from the store with runtime availability so gateway
// and CLI callers see consistent replicas/availableReplicas values.
func BuildFunctionStatus(client *containerd.Client, stored StoredFunction) types.FunctionStatus {
	availableReplicas := uint64(0)
	replicas := stored.DesiredReplicas
	if replicas == 0 {
		replicas = 1
	}

	if function, err := GetFunction(client, stored.Name, stored.Namespace); err == nil {
		if function.replicas > 0 {
			availableReplicas = 1
		}
		if stored.CreatedAt.IsZero() {
			stored.CreatedAt = function.createdAt
		}
	}

	labels := map[string]string{}
	for k, v := range stored.Labels {
		labels[k] = v
	}

	annotations := map[string]string{}
	for k, v := range stored.Annotations {
		annotations[k] = v
	}

	secrets := make([]string, len(stored.Secrets))
	copy(secrets, stored.Secrets)

	envVars := map[string]string{}
	for k, v := range stored.EnvVars {
		envVars[k] = v
	}

	status := types.FunctionStatus{
		Name:              stored.Name,
		Image:             stored.Image,
		Replicas:          replicas,
		AvailableReplicas: availableReplicas,
		Namespace:         stored.Namespace,
		Labels:            &labels,
		Annotations:       &annotations,
		Secrets:           secrets,
		EnvVars:           envVars,
		EnvProcess:        stored.EnvProcess,
		CreatedAt:         stored.CreatedAt,
	}

	if stored.Limits != nil {
		status.Limits = &types.FunctionResources{Memory: stored.Limits.Memory, CPU: stored.Limits.CPU}
	}

	return status
}

func PutFunctionFromDeployment(req types.FunctionDeployment, namespace string) StoredFunction {
	store := getFunctionStore()
	stored := NewStoredFunctionFromDeployment(req, namespace)
	store.Put(stored)
	return stored
}

func DeleteStoredFunction(namespace, name string) {
	getFunctionStore().Delete(namespace, name)
}

func GetStoredFunction(namespace, name string) (StoredFunction, bool) {
	return getFunctionStore().Get(namespace, name)
}

func ListStoredFunctions(namespace string) []StoredFunction {
	return getFunctionStore().List(namespace)
}

func ensureFunctionLabelsForAutoscaler(labels map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range labels {
		out[k] = v
	}
	return out
}
