package handlers

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/namespaces"
	gocni "github.com/containerd/go-cni"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"github.com/openfaas/faas-provider/types"
	"github.com/openfaas/faasd/pkg/cninetwork"
	"github.com/openfaas/faasd/pkg/service"
)

var (
	storeMu             sync.RWMutex
	activeFunctionStore FunctionStore = NewInMemoryFunctionStore()
	autoscalerMu        sync.RWMutex
	activeAutoScaler    *FaasdAutoScalerController
)

type FaasdAutoScalerController struct {
	client         *containerd.Client
	cni            gocni.CNI
	store          FunctionStore
	baseSecretPath string
	alwaysPull     bool
	logger         *slog.Logger
	autoScaler     *autoscaler.AutoScaler
}

type faasdScaleOperation struct {
	controller *FaasdAutoScalerController
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

func SetAutoScalerController(controller *FaasdAutoScalerController) {
	autoscalerMu.Lock()
	activeAutoScaler = controller
	autoscalerMu.Unlock()
}

func getAutoScalerController() *FaasdAutoScalerController {
	autoscalerMu.RLock()
	defer autoscalerMu.RUnlock()
	return activeAutoScaler
}

func NewFaasdAutoScalerController(client *containerd.Client, cni gocni.CNI, store FunctionStore, baseSecretPath string, alwaysPull bool, cfg autoscaler.Config, logger *slog.Logger) *FaasdAutoScalerController {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	controller := &FaasdAutoScalerController{
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

func (f *FaasdAutoScalerController) Start() {
	if f == nil || f.autoScaler == nil {
		return
	}
	f.autoScaler.Start()
}

func (f *FaasdAutoScalerController) Stop() {
	if f == nil || f.autoScaler == nil {
		return
	}
	f.autoScaler.Stop()
}

func (f *FaasdAutoScalerController) Enabled() bool {
	if f == nil || f.autoScaler == nil {
		return false
	}
	return f.autoScaler.IsEnabled()
}

func (f *FaasdAutoScalerController) RegisterFunction(namespace, name string, labels map[string]string) {
	f.RegisterFunctionWithState(namespace, name, labels, autoscaler.StateActive)
}

func (f *FaasdAutoScalerController) RegisterFunctionWithState(namespace, name string, labels map[string]string, state autoscaler.LifecycleState) {
	if !f.Enabled() {
		return
	}
	f.autoScaler.RegisterFunctionWithState(f.scaleKey(namespace, name), labels, state)
}

func (f *FaasdAutoScalerController) UnregisterFunction(namespace, name string) {
	if !f.Enabled() {
		return
	}
	f.autoScaler.UnregisterFunction(f.scaleKey(namespace, name))
}

func (f *FaasdAutoScalerController) RecordActivity(namespace, name string) {
	if !f.Enabled() {
		return
	}
	f.autoScaler.RecordActivity(f.scaleKey(namespace, name))
}

func (f *FaasdAutoScalerController) ScaleUp(namespace, name string) error {
	return f.ScaleUpWithMode(namespace, name, true)
}

func (f *FaasdAutoScalerController) ScaleUpWithMode(namespace, name string, cold bool) error {
	if f == nil {
		return fmt.Errorf("autoscaler controller not configured")
	}
	if !f.Enabled() {
		return nil
	}

	start := time.Now()

	if err := f.autoScaler.ScaleUpWhenReady(f.scaleKey(namespace, name)); err != nil {
		return err
	}

	if f.client == nil {
		return nil
	}

	if f.runtimeAvailable(namespace, name) {
		if callgraphController := getCallGraphController(); callgraphController != nil {
			labels := map[string]string(nil)
			if stored, ok := f.store.Get(namespace, name); ok {
				labels = stored.Labels
			}
			callgraphController.recordScaleUp(name, time.Since(start), cold)
			RegisterCallGraphFunction(f.client, namespace, name, labels)
		}
		return nil
	}

	if err := f.restoreRuntime(namespace, name); err != nil {
		return err
	}

	if callGraphController := getCallGraphController(); callGraphController != nil {
		labels := map[string]string(nil)
		if stored, ok := f.store.Get(namespace, name); ok {
			labels = stored.Labels
		}
		callGraphController.recordScaleUp(name, time.Since(start), cold)
		RegisterCallGraphFunction(f.client, namespace, name, labels)
	}

	return nil
}

func (f *FaasdAutoScalerController) ScaleDown(namespace, name string) error {
	if f == nil {
		return fmt.Errorf("autoscaler controller not configured")
	}

	start := time.Now()

	ctx := namespaces.WithNamespace(context.Background(), namespace)
	if function, err := GetFunction(f.client, name, namespace); err == nil {
		if function.replicas != 0 {
			if err := cninetwork.DeleteCNINetwork(ctx, f.cni, f.client, name); err != nil {
				f.logger.Error("error removing CNI network", "function", name, "namespace", namespace, "err", err)
			}
		}
	}

	if err := service.Remove(ctx, f.client, name); err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil
		}
		return err
	}

	if callGraphController := getCallGraphController(); callGraphController != nil {
		callGraphController.recordScaleDown(name, time.Since(start))
		callGraphController.markFunctionInactive(namespace, name)
	}

	return nil
}

func (f *FaasdAutoScalerController) ScaleDownWhenIdle(namespace, name string) error {
	if f == nil {
		return fmt.Errorf("autoscaler controller not configured")
	}
	if !f.Enabled() {
		return nil
	}
	return f.autoScaler.ScaleDownWhenIdle(f.scaleKey(namespace, name))
}

func (f *FaasdAutoScalerController) restoreRuntime(namespace, name string) error {
	if f == nil {
		return fmt.Errorf("autoscaler controller not configured")
	}
	if f.client == nil {
		return fmt.Errorf("containerd client not configured")
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

	return nil
}

func (f *FaasdAutoScalerController) runtimeAvailable(namespace, name string) bool {
	if f == nil || f.client == nil {
		return false
	}

	function, err := GetFunction(f.client, name, namespace)
	if err != nil {
		return false
	}

	return function.replicas > 0
}

func (f *FaasdAutoScalerController) StartInvocation(namespace, name string) error {
	if !f.Enabled() {
		return nil
	}
	return f.autoScaler.StartInvocation(f.scaleKey(namespace, name))
}

func (f *FaasdAutoScalerController) EndInvocation(namespace, name string) {
	if !f.Enabled() {
		return
	}
	f.autoScaler.EndInvocation(f.scaleKey(namespace, name))
}

func (f *FaasdAutoScalerController) scaleKey(namespace, name string) string {
	return namespace + "/" + name
}

func (f *FaasdAutoScalerController) parseScaleKey(key string) (string, string) {
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
	return f.controller.restoreRuntime(namespace, name)
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
