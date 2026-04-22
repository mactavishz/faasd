package integrations

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	testutil "github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const defaultCFSPeriod = uint64(100000)

type ResourceLimitsSuite struct {
	suite.Suite
	baseURL string
	auth    testutil.FaasdGatewayAuth
}

func TestResourceLimitsSuite(t *testing.T) {
	suite.Run(t, new(ResourceLimitsSuite))
}

func (s *ResourceLimitsSuite) SetupSuite() {
	s.baseURL, s.auth = testutil.RequireFaasd(s.T())
}

func (s *ResourceLimitsSuite) TestResourceLimitsInFaasd() {
	t := s.T()
	repoRoot := testutil.RepoRoot(t)
	fixtureDir := filepath.Join(repoRoot, "faasd", "test", "fns", "nodeinfo")

	fnName := testutil.UniqueFunctionName("faasd-nodeinfo-limits")
	fn := testutil.FixtureFunction(t, fixtureDir, "nodeinfo")
	t.Cleanup(func() { testutil.RemoveFunction(t, fnName, s.baseURL) })

	testutil.DeployFunction(t, s.baseURL, fnName, fn, testutil.DeployOptions{
		CPULimit:    "50m",
		MemoryLimit: "96Mi",
	})

	payload := []byte("verify resource limits")
	status, body := testutil.InvokeFaasdFunctionEventually(t, s.baseURL, s.auth, fnName, payload, http.StatusOK, 90*time.Second)
	require.Equal(t, http.StatusOK, status)
	assert.NotEmpty(t, body)

	deployedFn := testutil.WaitForFaasdFunction(t, s.baseURL, s.auth, fnName, 20*time.Second)
	require.NotNil(t, deployedFn.Limits, "expected limits in /system/functions")
	assert.Equal(t, "50m", deployedFn.Limits.CPU)
	assert.Equal(t, "96Mi", deployedFn.Limits.Memory)

	info := testutil.WaitForContainerInfo(t, fnName, 20*time.Second)
	require.NotNil(t, info.Spec.Linux.Resources.Memory.Limit)
	require.NotNil(t, info.Spec.Linux.Resources.CPU.Quota)
	require.NotNil(t, info.Spec.Linux.Resources.CPU.Period)

	expectedMemBytes := testutil.MemoryBytes(t, "96Mi")
	expectedNano := testutil.CPUNano(t, "50m")
	expectedQuota := testutil.CPUQuotaFromNano(expectedNano, defaultCFSPeriod)

	assert.Equal(t, expectedMemBytes, *info.Spec.Linux.Resources.Memory.Limit)
	assert.Equal(t, defaultCFSPeriod, *info.Spec.Linux.Resources.CPU.Period)
	assert.Equal(t, expectedQuota, *info.Spec.Linux.Resources.CPU.Quota)
}
