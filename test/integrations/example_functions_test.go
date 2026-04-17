package integrations

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/openfaas/faasd/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ExampleFunctionsSuite struct {
	suite.Suite
	baseURL string
	auth    testutil.GatewayAuth
}

func TestExampleFunctionsSuite(t *testing.T) {
	suite.Run(t, new(ExampleFunctionsSuite))
}

func (s *ExampleFunctionsSuite) SetupSuite() {
	s.baseURL, s.auth = testutil.RequireFaasd(s.T())
}

func (s *ExampleFunctionsSuite) TestRemoteImageNodeInfo() {
	t := s.T()
	repoRoot := testutil.RepoRoot(t)
	stackPath := filepath.Join(repoRoot, "faasd", "test", "fns", "nodeinfo", "stack.yaml")

	fnName := "nodeinfo"

	testutil.RemoveFunction(t, fnName, s.baseURL)
	t.Cleanup(func() { testutil.RemoveFunction(t, fnName, s.baseURL) })
	testutil.DeployStack(t, stackPath, s.baseURL)

	deployedFn := testutil.WaitForFunction(t, s.baseURL, s.auth, fnName, 20*time.Second)
	assert.Equal(t, fnName, deployedFn.Name)

	status, body := testutil.InvokeFunctionEventually(t, s.baseURL, s.auth, fnName, nil, http.StatusOK, 60*time.Second)
	bodyStr := string(body)
	t.Logf("invoke status: %d, body: \n%s", status, bodyStr)
	assert.NotEmpty(t, bodyStr)
	assert.Contains(t, bodyStr, "Hostname")
	assert.Contains(t, bodyStr, "Platform")
	assert.Contains(t, bodyStr, "Arch")
	assert.Contains(t, bodyStr, "CPUs")
}

func (s *ExampleFunctionsSuite) TestLocalBuildPushEchoJS() {
	t := s.T()
	testutil.RequireLocalRegistryReachable(t)

	repoRoot := testutil.RepoRoot(t)
	stackPath := filepath.Join(repoRoot, "faasd", "test", "fns", "echo-js", "stack.yaml")

	fnName := "echo-js"

	testutil.RemoveFunction(t, fnName, s.baseURL)
	t.Cleanup(func() { testutil.RemoveFunction(t, fnName, s.baseURL) })
	testutil.BuildStack(t, stackPath)
	testutil.PushStack(t, stackPath)
	testutil.DeployStack(t, stackPath, s.baseURL)

	payload := []byte("hello from local image")
	status, body := testutil.InvokeFunctionEventually(t, s.baseURL, s.auth, fnName, payload, http.StatusOK, 60*time.Second)
	require.Equal(t, http.StatusOK, status)

	t.Logf("invoke status: %d, body: \n%s", status, string(body))
	decoded := map[string]any{}
	require.NoError(t, json.Unmarshal(body, &decoded), "response is not valid JSON: %s", string(body))
	assert.Equal(t, string(payload), decoded["body"])

	headers, ok := decoded["headers"].(map[string]any)
	require.True(t, ok, "headers missing in response: %s", string(body))
	_, hasUserAgent := headers["user-agent"]
	assert.True(t, hasUserAgent, "expected user-agent header in response")
}
