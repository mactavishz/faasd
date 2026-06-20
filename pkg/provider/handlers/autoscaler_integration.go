package handlers

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
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
	_, err := f.ScaleUpWithMode(namespace, name, true)
	return err
}

// ScaleUpWithMode ensures a function's runtime is available and reports whether
// THIS call performed the scaled-down -> active transition. A demand-driven cold
// start (cold=true) blocks until ready; a speculative prewarm (cold=false) is
// opportunistic and returns immediately if another caller is already scaling the
// function, so it never occupies a prewarm slot waiting on work it did not
// initiate. Only the caller that performed the transition records the scale-up
// (with its own mode) and restores the runtime if needed.
func (f *FaasdAutoScalerController) ScaleUpWithMode(namespace, name string, cold bool) (bool, error) {
	if f == nil {
		return false, fmt.Errorf("autoscaler controller not configured")
	}
	if !f.Enabled() {
		return false, nil
	}

	// On a request-path cold start (cold=true), the caller's own scale-up is
	// otherwise-idle time we can use to warm its predicted synchronous downstream
	// functions, so they are ready (or nearly so) by the time the caller
	// dispatches to them. Fire this before blocking on ScaleUpWhenReady. Prewarms
	// (cold=false) are excluded to avoid recursion.
	if cold {
		if cg := getCallGraphController(); cg != nil {
			go cg.prewarmDownstreamEager(namespace, name)
		}
	}

	start := time.Now()
	key := f.scaleKey(namespace, name)
	var performed bool
	var err error
	if cold {
		performed, err = f.autoScaler.ScaleUpWhenReady(key)
	} else {
		performed, err = f.autoScaler.TryScaleUp(key)
	}
	if err != nil {
		return false, err
	}

	// Another caller (demand or another prewarm) is already driving this
	// function's scale-up, or it is already active: there is nothing for this
	// caller to record or restore.
	if !performed {
		return false, nil
	}

	if f.client == nil {
		return true, nil
	}

	if f.runtimeAvailable(namespace, name) {
		// Runtime came up (or was already up) without an explicit restore.
		f.recordScaleUpResult(namespace, name, cold, time.Since(start), false)
		return true, nil
	}

	if err := f.restoreRuntime(namespace, name); err != nil {
		return true, err
	}

	f.recordScaleUpResult(namespace, name, cold, time.Since(start), true)
	return true, nil
}

// recordScaleUpResult logs and records a completed scale-up. It is called only by
// the caller that performed the transition, so the duration sample reflects the
// real scale-up and is attributed to the correct mode (cold vs prewarm).
func (f *FaasdAutoScalerController) recordScaleUpResult(namespace, name string, cold bool, duration time.Duration, restored bool) {
	if f.client == nil {
		return
	}
	f.logScaleUp(name, cold, duration, restored)
	if cg := getCallGraphController(); cg != nil {
		cg.recordScaleUp(name, duration, cold)
		labels := map[string]string(nil)
		if stored, ok := f.store.Get(namespace, name); ok {
			labels = stored.Labels
		}
		RegisterCallGraphFunction(f.client, namespace, name, labels)
	}
}

// logScaleUp emits a structured scale-up record. cold=true marks a user-facing
// (request-path) cold start; cold=false marks a proactive prewarm.
func (f *FaasdAutoScalerController) logScaleUp(name string, cold bool, duration time.Duration, restored bool) {
	if f.logger == nil {
		return
	}
	f.logger.Info("scale-up completed",
		"function", name,
		"cold", cold,
		"restored", restored,
		"duration", duration)
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
	if stored.ArchiveBacked {
		snapshotter := ""
		if val, ok := os.LookupEnv("snapshotter"); ok {
			snapshotter = val
		}

		image, err := service.PrepareLocalImage(ctx, f.client, stored.Image, snapshotter)
		if err != nil {
			return err
		}

		return deployPreparedImage(ctx, req, f.client, f.cni, secretMountPath, image, snapshotter)
	}

	return deploy(ctx, req, f.client, f.cni, secretMountPath, f.alwaysPull, nil)
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
	return PutFunctionFromDeploymentWithSource(req, namespace, false)
}

func PutFunctionFromDeploymentWithSource(req types.FunctionDeployment, namespace string, archiveBacked bool) StoredFunction {
	store := getFunctionStore()
	stored := NewStoredFunctionFromDeploymentWithSource(req, namespace, archiveBacked)
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
