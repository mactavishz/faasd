package integrations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	testutil "github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type faasdFunctionStatsResponse struct {
	Function struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"function"`
	Summary struct {
		SuccessfulInvocations int            `json:"successful_invocations"`
		FailedInvocations     int            `json:"failed_invocations"`
		StatusCodes           map[string]int `json:"status_codes"`
	} `json:"summary"`
	Invocations []struct {
		StartedAt  time.Time `json:"started_at"`
		FinishedAt time.Time `json:"finished_at"`
		DurationNS int64     `json:"duration_ns"`
		Method     string    `json:"method"`
		Path       string    `json:"path"`
		StatusCode int       `json:"status_code"`
		Success    bool      `json:"success"`
	} `json:"invocations"`
}

type FunctionStatsSuite struct {
	suite.Suite
	baseURL string
	auth    testutil.FaasdGatewayAuth
}

func TestFunctionStatsSuite(t *testing.T) {
	suite.Run(t, new(FunctionStatsSuite))
}

func (s *FunctionStatsSuite) SetupSuite() {
	s.baseURL, s.auth = testutil.RequireFaasd(s.T())
}

func (s *FunctionStatsSuite) TestFunctionStats() {
	t := s.T()
	repoRoot := testutil.RepoRoot(t)
	stackPath := filepath.Join(repoRoot, "faasd", "test", "fns", "echo-js-remote", "stack.yaml")
	fnName := "echo-js-remote"

	testutil.RemoveFunction(t, fnName, s.baseURL)
	t.Cleanup(func() { testutil.RemoveFunction(t, fnName, s.baseURL) })
	testutil.DeployStack(t, stackPath, s.baseURL)
	testutil.WaitForFaasdFunction(t, s.baseURL, s.auth, fnName, 20*time.Second)
	requireFunctionStatsEndpointAvailable(t, s.baseURL, s.auth, fnName, "openfaas-fn")
	baseline := getFunctionStats(t, s.baseURL, s.auth, fnName, "openfaas-fn")

	for i := 0; i < 3; i++ {
		payload := []byte(fmt.Sprintf("sync-%d", i))
		status, body := testutil.InvokeFaasdFunction(t, s.baseURL, s.auth, fnName, bytes.NewReader(payload))
		require.Equal(t, http.StatusOK, status)
		assert.NotEmpty(t, body)
	}

	stats := waitForFunctionStatsCountGreaterEqual(t, s.baseURL, s.auth, fnName, "openfaas-fn", len(baseline.Invocations)+3, 120*time.Second)
	assert.Equal(t, fnName, stats.Function.Name)
	assert.Equal(t, "openfaas-fn", stats.Function.Namespace)
	assert.Equal(t, baseline.Summary.SuccessfulInvocations+3, stats.Summary.SuccessfulInvocations)
	assert.Equal(t, baseline.Summary.FailedInvocations, stats.Summary.FailedInvocations)
	assert.Equal(t, baseline.Summary.StatusCodes["200"]+3, stats.Summary.StatusCodes["200"])
	assert.Equal(t, len(baseline.Invocations)+3, len(stats.Invocations))

	latestSync := stats.Invocations[len(stats.Invocations)-3:]
	for _, invocation := range latestSync {
		assert.Equal(t, http.MethodPost, invocation.Method)
		assert.Equal(t, "/function/"+fnName, invocation.Path)
		assert.Equal(t, http.StatusOK, invocation.StatusCode)
		assert.True(t, invocation.Success)
		assert.False(t, invocation.FinishedAt.Before(invocation.StartedAt))
		assert.GreaterOrEqual(t, invocation.DurationNS, int64(0))
	}

	for i := 0; i < 2; i++ {
		status, body := testutil.InvokeFaasdAsyncJSON(t, s.baseURL, s.auth, fnName, map[string]any{"iteration": i}, "")
		require.Equal(t, http.StatusAccepted, status, "async enqueue body=%s", string(body))
	}

	stats = waitForFunctionStatsCountGreaterEqual(t, s.baseURL, s.auth, fnName, "openfaas-fn", len(baseline.Invocations)+5, 120*time.Second)
	assert.Equal(t, fnName, stats.Function.Name)
	assert.Equal(t, "openfaas-fn", stats.Function.Namespace)
	assert.Equal(t, baseline.Summary.SuccessfulInvocations+5, stats.Summary.SuccessfulInvocations)
	assert.Equal(t, baseline.Summary.FailedInvocations, stats.Summary.FailedInvocations)
	assert.Equal(t, baseline.Summary.StatusCodes["200"]+5, stats.Summary.StatusCodes["200"])
	assert.Zero(t, stats.Summary.StatusCodes["202"])
	assert.Len(t, stats.Invocations, len(baseline.Invocations)+5)

	latestAsync := stats.Invocations[len(stats.Invocations)-2:]
	for _, invocation := range latestAsync {
		assert.Equal(t, http.MethodPost, invocation.Method)
		assert.Contains(t, []string{"/function/" + fnName, "/function/" + fnName + "/"}, invocation.Path)
		assert.Equal(t, http.StatusOK, invocation.StatusCode)
		assert.True(t, invocation.Success)
		assert.False(t, invocation.FinishedAt.Before(invocation.StartedAt))
		assert.GreaterOrEqual(t, invocation.DurationNS, int64(0))
	}

	testutil.RemoveFunction(t, fnName, s.baseURL)
	waitForFunctionStatsStatus(t, s.baseURL, s.auth, fnName, "openfaas-fn", http.StatusNotFound, 30*time.Second)

	testutil.DeployStack(t, stackPath, s.baseURL)
	testutil.WaitForFaasdFunction(t, s.baseURL, s.auth, fnName, 20*time.Second)
	stats = getFunctionStats(t, s.baseURL, s.auth, fnName, "openfaas-fn")
	assert.Equal(t, fnName, stats.Function.Name)
	assert.Equal(t, "openfaas-fn", stats.Function.Namespace)
	assert.Equal(t, 0, stats.Summary.SuccessfulInvocations)
	assert.Equal(t, 0, stats.Summary.FailedInvocations)
	assert.Empty(t, stats.Summary.StatusCodes)
	assert.Empty(t, stats.Invocations)
}

func getFunctionStats(t *testing.T, baseURL string, auth testutil.FaasdGatewayAuth, functionName string, namespace string) faasdFunctionStatsResponse {
	t.Helper()

	status, body := getFunctionStatsOnce(t, baseURL, auth, functionName, namespace)
	require.Equal(t, http.StatusOK, status, "stats body=%s", string(body))

	var out faasdFunctionStatsResponse
	require.NoError(t, json.Unmarshal(body, &out), "invalid stats json: %s", string(body))
	return out
}

func getFunctionStatsOnce(t *testing.T, baseURL string, auth testutil.FaasdGatewayAuth, functionName string, namespace string) (int, []byte) {
	t.Helper()

	url := fmt.Sprintf("%s/system/stats/function/%s?namespace=%s", baseURL, functionName, namespace)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	secret := auth.Pass
	if secret == "" {
		secret = auth.Password
	}
	req.SetBasicAuth(auth.User, secret)

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

func waitForFunctionStatsCountGreaterEqual(t *testing.T, baseURL string, auth testutil.FaasdGatewayAuth, functionName string, namespace string, count int, timeout time.Duration) faasdFunctionStatsResponse {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var got faasdFunctionStatsResponse
	var lastStatus int
	var lastBody []byte

	for time.Now().Before(deadline) {
		status, body := getFunctionStatsOnce(t, baseURL, auth, functionName, namespace)
		lastStatus = status
		lastBody = body

		if status == http.StatusNotFound && bytes.Contains(bytes.ToLower(body), []byte("page not found")) {
			t.Fatalf("faasd stats endpoint unavailable for %q in namespace %q (status=%d body=%s). Rebuild faasd services to include /system/stats/function support.", functionName, namespace, status, strings.TrimSpace(string(body)))
		}
		if status != http.StatusOK {
			time.Sleep(time.Second)
			continue
		}
		if err := json.Unmarshal(body, &got); err != nil {
			time.Sleep(time.Second)
			continue
		}
		if got.Summary.StatusCodes == nil {
			got.Summary.StatusCodes = map[string]int{}
		}
		if len(got.Invocations) >= count {
			return got
		}
		time.Sleep(time.Second)
	}

	t.Fatalf("stats count did not reach %d within %s (last status=%d body=%s)", count, timeout, lastStatus, string(lastBody))

	if got.Summary.StatusCodes == nil {
		got.Summary.StatusCodes = map[string]int{}
	}
	return got
}

func waitForFunctionStatsStatus(t *testing.T, baseURL string, auth testutil.FaasdGatewayAuth, functionName string, namespace string, expectedStatus int, timeout time.Duration) {
	t.Helper()

	testutil.Eventually(t, timeout, time.Second, func() bool {
		status, _ := getFunctionStatsOnce(t, baseURL, auth, functionName, namespace)
		return status == expectedStatus
	})
}

func requireFunctionStatsEndpointAvailable(t *testing.T, baseURL string, auth testutil.FaasdGatewayAuth, functionName string, namespace string) {
	t.Helper()

	status, body := getFunctionStatsOnce(t, baseURL, auth, functionName, namespace)
	if status == http.StatusNotFound && bytes.Contains(bytes.ToLower(body), []byte("page not found")) {
		t.Fatalf("faasd stats endpoint unavailable for %q in namespace %q (status=%d body=%s). Rebuild faasd services to include /system/stats/function support.", functionName, namespace, status, strings.TrimSpace(string(body)))
	}
}
