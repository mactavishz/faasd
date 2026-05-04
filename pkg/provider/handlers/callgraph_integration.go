package handlers

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd"
	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
	"go.uber.org/zap"
)

const prewarmSafetyMargin = 50 * time.Millisecond

type callgraphRoute struct {
	namespace        string
	name             string
	ip               string
	active           bool
	callgraphEnabled bool
}

type invocationMeta struct {
	name   string
	callID string
	execID string
}

type FaasdCallGraphController struct {
	callGraphTracker callgraph.FullTracker
	autoscaler       *FaasdAutoScalerController
	client           *containerd.Client
	store            FunctionStore
	logger           *zap.Logger

	mu                  sync.RWMutex
	routingTable        map[string]*callgraphRoute
	reverseRoutingTable map[string]string
	invocationTable     sync.Map
}

var (
	callgraphMu     sync.RWMutex
	activeCallGraph *FaasdCallGraphController
)

func NewFaasdCallGraphController(autoscaler *FaasdAutoScalerController, client *containerd.Client, store FunctionStore, callGraphConfig callgraph.Config) *FaasdCallGraphController {
	logger := zap.NewNop()

	controller := &FaasdCallGraphController{
		autoscaler:          autoscaler,
		client:              client,
		store:               store,
		logger:              logger,
		routingTable:        make(map[string]*callgraphRoute),
		reverseRoutingTable: make(map[string]string),
	}

	controller.callGraphTracker = callgraph.New(
		callgraph.WithConfig(&callGraphConfig),
		callgraph.WithLogger(logger),
	)
	return controller
}

func SetCallGraphController(controller *FaasdCallGraphController) {
	callgraphMu.Lock()
	activeCallGraph = controller
	callgraphMu.Unlock()
}

func getCallGraphController() *FaasdCallGraphController {
	callgraphMu.RLock()
	defer callgraphMu.RUnlock()
	return activeCallGraph
}

func (c *FaasdCallGraphController) Start() {
	if c == nil || c.callGraphTracker == nil {
		return
	}
	c.callGraphTracker.Start()
}

func (c *FaasdCallGraphController) Stop() {
	if c == nil || c.callGraphTracker == nil {
		return
	}
	c.callGraphTracker.Stop()
}

func (c *FaasdCallGraphController) Enabled() bool {
	return c != nil && c.callGraphTracker != nil && c.callGraphTracker.Enabled()
}

func (c *FaasdCallGraphController) key(namespace, name string) string {
	return namespace + "/" + name
}

func (c *FaasdCallGraphController) upsertFunction(namespace, name string, labels map[string]string, ip string, active bool) {
	if !c.Enabled() || strings.TrimSpace(name) == "" {
		return
	}

	callgraphEnabled := callgraph.ParseCallgraphConfig(labels, "faasd", c.callGraphTracker)
	key := c.key(namespace, name)

	c.mu.Lock()
	defer c.mu.Unlock()

	route := &callgraphRoute{
		namespace:        namespace,
		name:             name,
		ip:               ip,
		active:           active,
		callgraphEnabled: callgraphEnabled,
	}

	if existing, ok := c.routingTable[key]; ok {
		if existing.ip != "" {
			delete(c.reverseRoutingTable, existing.ip)
		}
	}

	c.routingTable[key] = route
	if route.active && route.ip != "" {
		c.reverseRoutingTable[route.ip] = key
	}
}

func (c *FaasdCallGraphController) markFunctionInactive(namespace, name string) {
	if !c.Enabled() {
		return
	}

	key := c.key(namespace, name)

	c.mu.Lock()
	defer c.mu.Unlock()

	route, ok := c.routingTable[key]
	if !ok {
		return
	}

	if route.ip != "" {
		delete(c.reverseRoutingTable, route.ip)
	}
	route.active = false
	route.ip = ""
}

func (c *FaasdCallGraphController) deleteFunction(namespace, name string) {
	if !c.Enabled() {
		return
	}

	key := c.key(namespace, name)

	c.mu.Lock()
	if route, ok := c.routingTable[key]; ok {
		if route.ip != "" {
			delete(c.reverseRoutingTable, route.ip)
		}
	}
	delete(c.routingTable, key)
	c.mu.Unlock()

	c.callGraphTracker.ClearFunctionData(name)
}

func (c *FaasdCallGraphController) resetFunction(name string) {
	if !c.Enabled() {
		return
	}
	c.callGraphTracker.ResetFunctionStats(name)
}

func (c *FaasdCallGraphController) recordScaleUp(name string, duration time.Duration, cold bool) {
	if !c.Enabled() || strings.TrimSpace(name) == "" || duration <= 0 {
		return
	}
	c.callGraphTracker.RecordScaleUp(name, time.Now(), duration, cold)
}

func (c *FaasdCallGraphController) recordScaleDown(name string, duration time.Duration) {
	if !c.Enabled() || strings.TrimSpace(name) == "" || duration <= 0 {
		return
	}
	c.callGraphTracker.RecordScaleDown(name, time.Now(), duration)
}

// extractCaller attempts to determine the caller function name from the request's X-Forwarded-For header and the routing tables.
// It returns the caller function name (or empty string if external),
// a boolean indicating if a caller was found, and a boolean indicating if callgraph is enabled for the caller.
func (c *FaasdCallGraphController) extractCaller(r *http.Request) (string, bool, bool) {
	if !c.Enabled() || r == nil {
		return "", false, false
	}

	raw := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
	if raw == "" {
		return "", false, false
	}

	parts := strings.Split(raw, ",")
	if len(parts) == 0 {
		return "", false, false
	}

	ip := strings.TrimSpace(parts[0])
	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}
	if strings.HasPrefix(ip, "[") && strings.HasSuffix(ip, "]") {
		ip = strings.TrimPrefix(strings.TrimSuffix(ip, "]"), "[")
	}

	c.mu.RLock()
	key, ok := c.reverseRoutingTable[ip]
	if !ok {
		c.mu.RUnlock()
		return "", false, false
	}
	route := c.routingTable[key]
	c.mu.RUnlock()
	if route == nil {
		return "", false, false
	}

	return route.name, true, route.callgraphEnabled
}

// isCallgraphEnabled checks if callgraph tracking is enabled for the given function.
// If the function is not found in the routing table, it defaults to enabled to allow tracking of external entry points.
func (c *FaasdCallGraphController) isCallgraphEnabled(namespace string, name string) bool {
	if !c.Enabled() {
		return false
	}

	key := c.key(namespace, name)
	c.mu.RLock()
	route, ok := c.routingTable[key]
	c.mu.RUnlock()
	if !ok || route == nil {
		return true
	}

	return route.callgraphEnabled
}

func (c *FaasdCallGraphController) StartInvocation(r *http.Request, namespace, functionName string) {
	if r == nil {
		return
	}

	if !c.isCallgraphEnabled(namespace, functionName) {
		return
	}

	now := time.Now()
	callID := strings.TrimSpace(r.Header.Get("X-Call-Id"))
	if callID == "" {
		callID = uuid.New().String()
		r.Header.Set("X-Call-Id", callID)
	}

	callerExecID := strings.TrimSpace(r.Header.Get("X-Exec-Id"))
	caller, callerFound, callerEnabled := c.extractCaller(r)
	effectiveCaller := ""
	if callerFound && callerEnabled {
		effectiveCaller = caller
	}

	c.callGraphTracker.RecordEdge(effectiveCaller, functionName, callID, callerExecID, now)

	execID := uuid.New().String()
	r.Header.Set("X-Exec-Id", execID)
	c.callGraphTracker.StartExecution(functionName, callID, execID, now)
	// After starting execution, we can prewarm downstream functions based on the call graph data
	// NOTE: We assume that the functions in the same workflow also have the same namespace
	go c.prewarmDownstream(namespace, functionName)

	c.invocationTable.Store(r, invocationMeta{name: functionName, callID: callID, execID: execID})
}

func (c *FaasdCallGraphController) EndInvocation(r *http.Request, _, _ string) {
	if !c.Enabled() || r == nil {
		return
	}

	value, ok := c.invocationTable.Load(r)
	if !ok {
		return
	}
	c.invocationTable.Delete(r)

	meta, ok := value.(invocationMeta)
	if !ok {
		return
	}

	c.callGraphTracker.EndExecution(meta.name, meta.callID, meta.execID, time.Now())
}

func (c *FaasdCallGraphController) prewarmDownstream(namespace string, functionName string) {
	if !c.Enabled() || c.autoscaler == nil || !c.autoscaler.Enabled() || !c.callGraphTracker.PrewarmEnabled() {
		return
	}

	targets := c.callGraphTracker.GetPrewarmTargets(functionName)
	for _, target := range targets {
		c.schedulePrewarm(namespace, target)
	}
}

func (c *FaasdCallGraphController) schedulePrewarm(namespace string, target callgraph.PrewarmTarget) {
	if !c.isCallgraphEnabled(namespace, target.FunctionName) {
		return
	}

	if c.autoscaler.runtimeAvailable(namespace, target.FunctionName) {
		return
	}

	var coldStartTime time.Duration
	if stats, ok := c.callGraphTracker.GetFunctionStats(target.FunctionName); ok {
		coldStartTime = stats.AvgColdStartDuration
	}

	delay := target.LeadTime - coldStartTime - prewarmSafetyMargin
	if delay <= 0 {
		go c.executePrewarm(namespace, target.FunctionName)
		return
	}

	time.AfterFunc(delay, func() {
		if c.autoscaler.runtimeAvailable(namespace, target.FunctionName) {
			return
		}
		c.executePrewarm(namespace, target.FunctionName)
	})
}

func (c *FaasdCallGraphController) executePrewarm(namespace, functionName string) {
	if c.autoscaler == nil || !c.autoscaler.Enabled() {
		return
	}

	if err := c.autoscaler.ScaleUpWithMode(namespace, functionName, false); err != nil {
		c.logger.Debug("prewarm scale-up failed", zap.String("function", functionName), zap.Error(err))
		return
	}
}

func RegisterCallGraphFunction(client *containerd.Client, namespace, name string, labels map[string]string) {
	c := getCallGraphController()
	if c == nil || !c.Enabled() {
		return
	}

	fn, err := GetFunction(client, name, namespace)
	if err != nil {
		c.upsertFunction(namespace, name, labels, "", false)
		return
	}

	c.upsertFunction(namespace, name, labels, fn.IP, fn.replicas > 0)
}

func MakeCallGraphHandler(controller *FaasdCallGraphController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if controller == nil || controller.callGraphTracker == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(controller.callGraphTracker.GetCallGraph()); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func MakeCallGraphFunctionHandler(controller *FaasdCallGraphController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if controller == nil || controller.callGraphTracker == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		function := strings.TrimPrefix(r.URL.Path, "/system/callgraph/function/")
		if function == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		stats, ok := controller.callGraphTracker.GetFunctionStats(function)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func MakeCallGraphEdgeHandler(controller *FaasdCallGraphController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if controller == nil || controller.callGraphTracker == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		caller := strings.TrimSpace(r.URL.Query().Get("caller"))
		callee := strings.TrimSpace(r.URL.Query().Get("callee"))
		if callee == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		edge, ok := controller.callGraphTracker.GetEdgeStats(caller, callee)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(edge); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}
