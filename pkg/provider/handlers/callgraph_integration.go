package handlers

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd"
	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
)

const prewarmSafetyMargin = 50 * time.Millisecond

// defaultMaxConcurrentPrewarms bounds how many speculative prewarm scale-ups may
// run at once. Prewarming is best-effort: when the budget is exhausted, targets
// are skipped rather than queued, so speculative work never piles up against
// request-path cold starts. Overridable via FAASD_MAX_CONCURRENT_PREWARMS.
const defaultMaxConcurrentPrewarms = 3

func maxConcurrentPrewarms() int {
	if v := strings.TrimSpace(os.Getenv("FAASD_MAX_CONCURRENT_PREWARMS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultMaxConcurrentPrewarms
}

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
	logger           *slog.Logger

	mu                  sync.RWMutex
	routingTable        map[string]*callgraphRoute
	reverseRoutingTable map[string]string
	invocationTable     sync.Map

	// prewarmSem bounds concurrent speculative prewarm scale-ups so they cannot
	// starve request-path cold starts (best-effort, non-blocking).
	prewarmSem chan struct{}

	// prewarmInFlight collapses duplicate prewarm attempts for the same target
	// (e.g. eager + scheduled) so they do not each consume a global prewarm slot
	// for what the autoscaler coalesces into a single scale-up.
	prewarmInFlight sync.Map
}

var (
	callgraphMu     sync.RWMutex
	activeCallGraph *FaasdCallGraphController
)

func NewFaasdCallGraphController(autoscaler *FaasdAutoScalerController, client *containerd.Client, store FunctionStore, callGraphConfig callgraph.Config, logger *slog.Logger) *FaasdCallGraphController {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	controller := &FaasdCallGraphController{
		autoscaler:          autoscaler,
		client:              client,
		store:               store,
		logger:              logger,
		routingTable:        make(map[string]*callgraphRoute),
		reverseRoutingTable: make(map[string]string),
		prewarmSem:          make(chan struct{}, maxConcurrentPrewarms()),
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

	callID := strings.TrimSpace(r.Header.Get("X-Call-Id"))
	callerExecID := strings.TrimSpace(r.Header.Get("X-Exec-Id"))
	if callID != "" && callerExecID != "" {
		// If both X-Call-Id and X-Exec-Id are absent,
		// It is likely that the request is queued for the async invocation and the caller information is not propagated via headers.
		if caller, ok := c.callGraphTracker.GetExecutionContextFunction(callID, callerExecID); ok {
			return c.lookupRouteByFunctionName(caller)
		}
	}

	raw := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
	if raw == "" {
		return "", false, false
	}

	parts := strings.Split(raw, ",")
	if len(parts) == 0 {
		return "", false, false
	}

	normalizedIPs := make([]string, 0, len(parts))
	for _, part := range parts {
		ip := normalizeForwardedIP(part)
		if ip == "" {
			continue
		}
		normalizedIPs = append(normalizedIPs, ip)
	}

	if len(normalizedIPs) == 0 {
		return "", false, false
	}

	for idx := len(normalizedIPs) - 1; idx >= 0; idx-- {
		if caller, found, enabled := c.lookupRouteByIP(normalizedIPs[idx]); found {
			return caller, found, enabled
		}
	}

	return "", false, false
}

func (c *FaasdCallGraphController) lookupRouteByIP(ip string) (string, bool, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key, ok := c.reverseRoutingTable[ip]
	if !ok {
		return "", false, false
	}

	route := c.routingTable[key]
	if route == nil {
		return "", false, false
	}

	return route.name, true, route.callgraphEnabled
}

func (c *FaasdCallGraphController) lookupRouteByFunctionName(name string) (string, bool, bool) {
	if strings.TrimSpace(name) == "" {
		return "", false, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, route := range c.routingTable {
		if route == nil || route.name != name {
			continue
		}
		return route.name, true, route.callgraphEnabled
	}

	return name, true, true
}

func normalizeForwardedIP(value string) string {
	ip := strings.TrimSpace(value)
	if ip == "" {
		return ""
	}

	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}

	if strings.HasPrefix(ip, "[") && strings.HasSuffix(ip, "]") {
		ip = strings.TrimPrefix(strings.TrimSuffix(ip, "]"), "[")
	}

	return strings.TrimSpace(ip)
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

	// The caller marks async hops with the X-Faas-Async header (set per-call, and
	// absent on sync hops). Its presence therefore means an async edge; absence
	// means sync, which is also correct for external entry points (the client
	// awaits the response).
	kind := callgraph.EdgeKindSync
	if strings.TrimSpace(r.Header.Get("X-Faas-Async")) != "" {
		kind = callgraph.EdgeKindAsync
	}

	// Use the gateway arrival time (X-Faas-Arrival, host-clock ms) as the edge
	// timestamp, not `now`. By the time this runs the gateway has already scaled
	// the callee from zero, so `now` would bake the callee's cold start into the
	// lead time; the gateway stamps arrival before that, yielding a clean
	// caller->call-issue gap (matching tinyFaaS). Set by the gateway, so the
	// function workload needs no instrumentation. Falls back to `now` when the
	// header is absent (e.g. external entry points). StartExecution keeps `now`
	// because that marks when this function begins executing, which is the
	// correct reference for *its* downstream edges.
	edgeTime := now
	if arrival := parseArrival(r.Header.Get("X-Faas-Arrival")); !arrival.IsZero() {
		edgeTime = arrival
	}
	c.callGraphTracker.RecordEdgeWithKind(effectiveCaller, functionName, callID, callerExecID, edgeTime, kind)
	c.logger.Info("callgraph edge recorded",
		"caller", effectiveCaller,
		"callee", functionName,
		"kind", kind.String(),
		"callID", callID)

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
	c.logger.Info("prewarming downstream functions",
		"caller", functionName,
		"targetCount", len(targets))
	for _, target := range targets {
		c.schedulePrewarm(namespace, target)
	}
}

// prewarmDownstreamEager warms the predicted SYNCHRONOUS downstream functions of
// a caller that is itself cold-starting on the request path. Unlike
// schedulePrewarm it fires immediately instead of computing a delay against the
// recorded lead time: the caller's in-progress cold start is the lead time, and
// it is far larger than the tiny gap before the caller issues its first sync
// call (which is why those prewarms otherwise land "too late"). Asynchronous
// callees are skipped -- the caller does not block on them, so warming them here
// would only add contention to the caller's own in-flight cold start. Warms are
// bounded by prewarmSem and skipped (not queued) when exhausted. Fire-and-forget.
func (c *FaasdCallGraphController) prewarmDownstreamEager(namespace, functionName string) {
	if !c.Enabled() || c.autoscaler == nil || !c.autoscaler.Enabled() || !c.callGraphTracker.PrewarmEnabled() {
		return
	}

	targets := c.callGraphTracker.GetPrewarmTargets(functionName)
	if len(targets) == 0 {
		return
	}

	// Synchronous callees first (on the critical path), then the most imminent
	// (smallest lead time) first.
	sort.SliceStable(targets, func(i, j int) bool {
		si := targets[i].Kind == callgraph.EdgeKindSync
		sj := targets[j].Kind == callgraph.EdgeKindSync
		if si != sj {
			return si
		}
		return targets[i].LeadTime < targets[j].LeadTime
	})

	for _, target := range targets {
		// Skip async callees (off the caller's user-visible critical path). They
		// are cold-started on their own dispatch path. EdgeKindUnknown is treated
		// as potentially-critical and kept.
		if target.Kind == callgraph.EdgeKindAsync {
			continue
		}
		if !c.isCallgraphEnabled(namespace, target.FunctionName) {
			continue
		}
		if c.autoscaler.runtimeAvailable(namespace, target.FunctionName) {
			continue
		}

		// The lead time is now the clean caller->call-issue gap (edge stamped at
		// the caller's call-issue time). The caller's own cold start is extra lead
		// time on top of that gap:
		// delay = callerColdStart + leadTime - calleeColdStart - margin.
		delay := target.CallerColdStartDuration + target.LeadTime - target.AvgColdStartDuration - prewarmSafetyMargin

		if delay <= 0 {
			// Cannot be ready by waiting -- warm it now.
			c.startEagerPrewarm(namespace, functionName, target.FunctionName, delay, false)
			continue
		}

		// Enough runway to warm just in time. Schedule it, acquiring the budget at
		// fire time (not now) so the slot is not held during the wait.
		c.logger.Info("scheduling eager prewarm",
			"caller", functionName,
			"target", target.FunctionName,
			"callerColdStart", target.CallerColdStartDuration,
			"leadTime", target.LeadTime,
			"calleeColdStart", target.AvgColdStartDuration,
			"delay", delay)
		tname := target.FunctionName
		time.AfterFunc(delay, func() {
			if c.autoscaler.runtimeAvailable(namespace, tname) {
				return
			}
			c.startEagerPrewarm(namespace, functionName, tname, delay, true)
		})
	}
}

// parseArrival decodes the X-Faas-Arrival header (host-clock milliseconds since
// the Unix epoch, stamped by the gateway) into a time. Returns the zero time if
// absent/invalid.
func parseArrival(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// startEagerPrewarm kicks off an eager prewarm. Admission control (the shared
// prewarm budget) is enforced in executePrewarm, so eager and scheduled prewarms
// compete for the same global budget.
func (c *FaasdCallGraphController) startEagerPrewarm(namespace, caller, target string, delay time.Duration, scheduled bool) {
	c.logger.Info("eager prewarm during cold start",
		"caller", caller, "target", target, "delay", delay, "scheduled", scheduled)
	go c.executePrewarm(namespace, target)
}

func (c *FaasdCallGraphController) schedulePrewarm(namespace string, target callgraph.PrewarmTarget) {
	if !c.isCallgraphEnabled(namespace, target.FunctionName) {
		return
	}

	if c.autoscaler.runtimeAvailable(namespace, target.FunctionName) {
		c.logger.Info("skipping prewarm - function already active",
			"target", target.FunctionName,
			"kind", target.Kind.String())
		return
	}

	var coldStartTime time.Duration
	if stats, ok := c.callGraphTracker.GetFunctionStats(target.FunctionName); ok {
		coldStartTime = stats.AvgColdStartDuration
	}

	// The lead time is the clean caller->call-issue gap. The caller is already
	// warm on this path, so delay = leadTime - calleeColdStart - margin.
	delay := target.LeadTime - coldStartTime - prewarmSafetyMargin
	if delay <= 0 {
		// The cold start cannot finish within the observed lead time, so firing
		// now would not make the function ready in time -- it would only add
		// container-start contention while racing (and losing to) the on-demand
		// cold start. Skip it. The eager cold-start path (prewarmDownstreamEager)
		// covers these tight critical-path lead times by warming the caller's
		// synchronous downstream while the caller itself is still cold-starting.
		c.logger.Info("skipping prewarm - predicted too late",
			"target", target.FunctionName,
			"kind", target.Kind.String(),
			"leadTime", target.LeadTime,
			"coldStartTime", coldStartTime,
			"delay", delay)
		return
	}

	c.logger.Info("scheduling prewarm",
		"target", target.FunctionName,
		"kind", target.Kind.String(),
		"leadTime", target.LeadTime,
		"coldStartTime", coldStartTime,
		"delay", delay)

	time.AfterFunc(delay, func() {
		if c.autoscaler.runtimeAvailable(namespace, target.FunctionName) {
			c.logger.Info("skipping scheduled prewarm - function became active",
				"target", target.FunctionName)
			return
		}
		c.executePrewarm(namespace, target.FunctionName)
	})
}

func (c *FaasdCallGraphController) executePrewarm(namespace, functionName string) {
	if c.autoscaler == nil || !c.autoscaler.Enabled() {
		return
	}

	// Per-target admission first: if a prewarm for this function is already in
	// flight, skip -- otherwise duplicate attempts (eager + scheduled) would each
	// take a global slot for a scale-up the autoscaler coalesces into one.
	if _, inFlight := c.prewarmInFlight.LoadOrStore(functionName, struct{}{}); inFlight {
		return
	}
	defer c.prewarmInFlight.Delete(functionName)

	// Global admission control for all speculative prewarms (eager and scheduled
	// alike). Best-effort: if the budget is exhausted, skip rather than queue, so
	// speculative work never competes with request-path cold starts. Request-path
	// cold starts (ScaleUpWithMode with cold=true) do not go through here and are
	// never throttled.
	select {
	case c.prewarmSem <- struct{}{}:
	default:
		c.logger.Info("skipping prewarm - budget exhausted", "function", functionName)
		return
	}
	defer func() { <-c.prewarmSem }()

	start := time.Now()
	performed, err := c.autoscaler.ScaleUpWithMode(namespace, functionName, false)
	if err != nil {
		c.logger.Warn("prewarm scale-up failed", "function", functionName, "err", err)
		return
	}
	if !performed {
		// Another caller was already scaling this function (demand or another
		// prewarm); nothing was done, so do not log a completion.
		return
	}
	c.logger.Info("prewarm downstream function completed",
		"function", functionName,
		"duration", time.Since(start))
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
