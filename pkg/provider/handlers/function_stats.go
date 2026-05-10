package handlers

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/containerd/containerd"
	"github.com/gorilla/mux"
)

type InvocationRecord struct {
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	DurationNS int64     `json:"duration_ns"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	StatusCode int       `json:"status_code"`
	Success    bool      `json:"success"`
}

type FunctionSummary struct {
	SuccessfulInvocations int            `json:"successful_invocations"`
	FailedInvocations     int            `json:"failed_invocations"`
	StatusCodes           map[string]int `json:"status_codes"`
}

type FunctionStats struct {
	Invocations []InvocationRecord
	Summary     FunctionSummary
}

type FunctionStatsResponse struct {
	Function struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"function"`
	Summary     FunctionSummary    `json:"summary"`
	Invocations []InvocationRecord `json:"invocations"`
}

type functionStatsStore struct {
	mu    sync.RWMutex
	stats map[string]FunctionStats
}

var faasdFunctionStats = &functionStatsStore{stats: map[string]FunctionStats{}}

func (s *functionStatsStore) Record(namespace, name string, record InvocationRecord) {
	if s == nil {
		return
	}

	key := storeKey(namespace, name)
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.stats[key]
	stats.Invocations = append(stats.Invocations, record)
	if stats.Summary.StatusCodes == nil {
		stats.Summary.StatusCodes = map[string]int{}
	}
	if record.Success {
		stats.Summary.SuccessfulInvocations++
	} else {
		stats.Summary.FailedInvocations++
	}
	stats.Summary.StatusCodes[strconv.Itoa(record.StatusCode)]++
	s.stats[key] = stats
}

func (s *functionStatsStore) Get(namespace, name string) (FunctionStats, bool) {
	if s == nil {
		return FunctionStats{}, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	stats, ok := s.stats[storeKey(namespace, name)]
	if !ok {
		return FunctionStats{}, false
	}
	return cloneFunctionStats(stats), true
}

func (s *functionStatsStore) Reset(namespace, name string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.stats, storeKey(namespace, name))
}

func cloneFunctionStats(in FunctionStats) FunctionStats {
	out := FunctionStats{
		Invocations: make([]InvocationRecord, len(in.Invocations)),
		Summary: FunctionSummary{
			SuccessfulInvocations: in.Summary.SuccessfulInvocations,
			FailedInvocations:     in.Summary.FailedInvocations,
			StatusCodes:           map[string]int{},
		},
	}
	copy(out.Invocations, in.Invocations)
	for code, count := range in.Summary.StatusCodes {
		out.Summary.StatusCodes[code] = count
	}
	return out
}

type statusCapturingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (w *statusCapturingResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.written = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *statusCapturingResponseWriter) Write(b []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func MakeFunctionStatsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		functionName := vars["name"]
		if functionName == "" {
			next(w, r)
			return
		}

		actualName, namespace := ParseFunctionNameNamespace(functionName)
		if namespace == "" {
			namespace = getRequestNamespace(readNamespaceFromQuery(r))
		}

		startedAt := time.Now().UTC()
		capturing := &statusCapturingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next(capturing, r)

		finishedAt := time.Now().UTC()
		record := InvocationRecord{
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
			DurationNS: finishedAt.Sub(startedAt).Nanoseconds(),
			Method:     r.Method,
			Path:       r.URL.Path,
			StatusCode: capturing.statusCode,
			Success:    capturing.statusCode >= 200 && capturing.statusCode < 400,
		}
		faasdFunctionStats.Record(namespace, actualName, record)
	}
}

func MakeFunctionStatsHandler(client *containerd.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		vars := mux.Vars(r)
		name := vars["name"]
		namespace := getRequestNamespace(readNamespaceFromQuery(r))

		valid, err := validNamespace(client.NamespaceService(), namespace)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !valid {
			http.Error(w, "namespace not valid", http.StatusBadRequest)
			return
		}

		stats, ok := faasdFunctionStats.Get(namespace, name)
		_, hasStored := GetStoredFunction(namespace, name)
		if !hasStored {
			if _, err := GetFunction(client, name, namespace); err == nil {
				hasStored = true
			}
		}
		if !ok && !hasStored {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !ok {
			stats = FunctionStats{Summary: FunctionSummary{StatusCodes: map[string]int{}}, Invocations: []InvocationRecord{}}
		}

		sort.Slice(stats.Invocations, func(i, j int) bool {
			return stats.Invocations[i].StartedAt.Before(stats.Invocations[j].StartedAt)
		})

		resp := FunctionStatsResponse{Summary: stats.Summary, Invocations: stats.Invocations}
		resp.Function.Name = name
		resp.Function.Namespace = namespace
		if resp.Summary.StatusCodes == nil {
			resp.Summary.StatusCodes = map[string]int{}
		}
		if resp.Invocations == nil {
			resp.Invocations = []InvocationRecord{}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func ResetFunctionStats(namespace, name string) {
	faasdFunctionStats.Reset(namespace, name)
}
