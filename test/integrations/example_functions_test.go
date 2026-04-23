package integrations

import (
	"bytes"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	testutil "github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ExampleFunctionsSuite struct {
	suite.Suite
	baseURL string
	auth    testutil.FaasdGatewayAuth
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
	stackPath := filepath.Join(repoRoot, "faasd", "test", "fns", "echo-js-remote", "stack.yaml")

	fnName := "echo-js-remote"

	testutil.RemoveFunction(t, fnName, s.baseURL)
	t.Cleanup(func() { testutil.RemoveFunction(t, fnName, s.baseURL) })
	testutil.DeployStack(t, stackPath, s.baseURL)

	deployedFn := testutil.WaitForFaasdFunction(t, s.baseURL, s.auth, fnName, 20*time.Second)
	assert.Equal(t, fnName, deployedFn.Name)

	payload := []byte("hello from local image")
	status, body := testutil.InvokeFaasdFunction(t, s.baseURL, s.auth, fnName, bytes.NewReader(payload))
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
	status, body := testutil.InvokeFaasdFunction(t, s.baseURL, s.auth, fnName, bytes.NewReader(payload))
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
