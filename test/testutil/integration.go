package testutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	sdkstack "github.com/openfaas/go-sdk/stack"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	defaultGatewayURL = "http://127.0.0.1:8080"
	vagrantVMName     = "faasd"
)

type GatewayAuth struct {
	User     string
	Password string
}

type FunctionResources struct {
	Memory string `json:"memory"`
	CPU    string `json:"cpu"`
}

type FunctionStatus struct {
	Name   string             `json:"name"`
	Image  string             `json:"image"`
	Limits *FunctionResources `json:"limits,omitempty"`
}

type DeployOptions struct {
	Image       string
	CPULimit    string
	MemoryLimit string
}

type ContainerInfo struct {
	ID   string `json:"ID"`
	Spec struct {
		Linux struct {
			Resources struct {
				Memory struct {
					Limit *int64 `json:"limit"`
				} `json:"memory"`
				CPU struct {
					Quota  *int64  `json:"quota"`
					Period *uint64 `json:"period"`
				} `json:"cpu"`
			} `json:"resources"`
		} `json:"linux"`
	} `json:"Spec"`
}

func RequireFaasd(t *testing.T) (string, GatewayAuth) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	RequireVagrant(t)
	RequireVMRunning(t, vagrantVMName)
	baseURL := GatewayURL()
	WaitForGateway(t, baseURL, 90*time.Second)
	RequireFaasCLI(t)
	LoginFaasCLI(t, baseURL)

	auth := ReadGatewayAuth(t)
	return baseURL, auth
}

func GatewayURL() string {
	if v := strings.TrimSpace(os.Getenv("FAASD_TEST_GATEWAY_URL")); len(v) > 0 {
		return strings.TrimRight(v, "/")
	}
	return defaultGatewayURL
}

func RepoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)

	for {
		if _, err := os.Stat(filepath.Join(dir, "Vagrantfile")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("failed to locate repository root from %s", dir)
		}
		dir = parent
	}
}

func RequireVagrant(t *testing.T) {
	t.Helper()
	_, err := exec.LookPath("vagrant")
	require.NoError(t, err, "vagrant not found in PATH")
}

func RequireFaasCLI(t *testing.T) {
	t.Helper()
	_, err := exec.LookPath("faas-cli")
	require.NoError(t, err, "faas-cli not found in PATH")
}

func RequireVMRunning(t *testing.T, vmName string) {
	t.Helper()

	out := MustCommand(t, 30*time.Second, RepoRoot(t), nil, "vagrant", "status", vmName, "--machine-readable")
	needle := "," + vmName + ",state,running"
	if !strings.Contains(out, needle) {
		t.Fatalf("vagrant VM %q is not running. Run: vagrant up %s", vmName, vmName)
	}
}

func WaitForGateway(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()

	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		url := strings.TrimRight(baseURL, "/") + "/system/functions"
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("failed to create gateway probe request: %v", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("gateway at %s is not reachable within %s", baseURL, timeout)
}

func ReadGatewayAuth(t *testing.T) GatewayAuth {
	t.Helper()

	user := strings.TrimSpace(VagrantSSH(t, vagrantVMName, "sudo cat /var/lib/faasd/secrets/basic-auth-user"))
	pass := strings.TrimSpace(VagrantSSH(t, vagrantVMName, "sudo cat /var/lib/faasd/secrets/basic-auth-password"))

	require.NotEmpty(t, user, "basic-auth-user is empty")
	require.NotEmpty(t, pass, "basic-auth-password is empty")

	return GatewayAuth{User: user, Password: pass}
}

func LoginFaasCLI(t *testing.T, gateway string) {
	t.Helper()

	repoRoot := RepoRoot(t)
	command := fmt.Sprintf("vagrant ssh %s -c \"sudo cat /var/lib/faasd/secrets/basic-auth-password\" | faas-cli login --password-stdin --gateway %s", vagrantVMName, gateway)
	MustCommand(t, 60*time.Second, repoRoot, nil, "sh", "-c", command)
}

func RequireLocalRegistryReachable(t *testing.T) {
	t.Helper()

	repoRoot := RepoRoot(t)
	_, err := TryCommand(20*time.Second, repoRoot, "docker", "ps")
	if err != nil {
		t.Skip("docker is not available on host; local build/push integration test requires docker")
	}

	statusHost, hostErr := TryCommand(20*time.Second, repoRoot, "sh", "-c", "curl -s -o /dev/null -w '%{http_code}' http://registry.local:5050/v2/")
	if hostErr != nil {
		t.Skipf("registry.local:5050 is not reachable from host: %v", hostErr)
	}
	hostCode := strings.TrimSpace(statusHost)
	if hostCode != "200" && hostCode != "401" {
		t.Skipf("registry.local:5050 returned unexpected status from host: %q", hostCode)
	}
}

func VagrantSSH(t *testing.T, vmName string, command string) string {
	t.Helper()
	out := MustCommand(t, 2*time.Minute, RepoRoot(t), nil, "vagrant", "ssh", vmName, "-c", command)
	return out
}

func MustCommand(t *testing.T, timeout time.Duration, workdir string, stdin []byte, name string, args ...string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	if workdir != "" {
		cmd.Dir = workdir
	}
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "command failed: %s %s\noutput:\n%s", name, strings.Join(args, " "), string(out))
	return string(out)
}

func TryCommand(timeout time.Duration, workdir string, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	if workdir != "" {
		cmd.Dir = workdir
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func UniqueFunctionName(prefix string) string {
	p := strings.ToLower(strings.TrimSpace(prefix))
	p = strings.ReplaceAll(p, "_", "-")
	p = strings.ReplaceAll(p, " ", "-")
	if p == "" {
		p = "fn"
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	name := p + "-" + suffix[len(suffix)-8:]
	if len(name) > 63 {
		name = name[:63]
		name = strings.TrimRight(name, "-")
	}
	return name
}

func ParseFixtureStack(t *testing.T, fixtureDir string) *sdkstack.Services {
	t.Helper()

	stackPath := filepath.Join(fixtureDir, "stack.yaml")
	services, err := sdkstack.ParseYAMLFile(stackPath, "", "", false)
	require.NoError(t, err)
	require.NotNil(t, services)
	return services
}

func FixtureFunction(t *testing.T, fixtureDir string, sourceName string) sdkstack.Function {
	t.Helper()

	services := ParseFixtureStack(t, fixtureDir)
	fn, ok := services.Functions[sourceName]
	require.True(t, ok, "source function %q not found in stack", sourceName)
	fn.Name = sourceName
	return fn
}

func BuildStack(t *testing.T, stackPath string) {
	t.Helper()
	t.Logf("building stack with: faas-cli build -f %s", stackPath)
	MustCommand(t, 10*time.Minute, filepath.Dir(stackPath), nil, "faas-cli", "build", "-f", stackPath)
}

func PushStack(t *testing.T, stackPath string) {
	t.Helper()
	t.Logf("pushing stack with: faas-cli push -f %s", stackPath)
	MustCommand(t, 10*time.Minute, filepath.Dir(stackPath), nil, "faas-cli", "push", "-f", stackPath)
}

func DeployStack(t *testing.T, stackPath string, gateway string) {
	t.Helper()
	t.Logf("deploying stack with: faas-cli deploy --gateway %s -f %s", gateway, stackPath)
	str := MustCommand(t, 10*time.Minute, filepath.Dir(stackPath), nil, "faas-cli", "deploy", "--gateway", gateway, "-f", stackPath)
	t.Logf("deploy output:\n%s", str)
}

func DeployFunction(t *testing.T, gateway string, functionName string, fn sdkstack.Function, opts DeployOptions) {
	t.Helper()

	image := strings.TrimSpace(opts.Image)
	if image == "" {
		image = strings.TrimSpace(fn.Image)
	}
	require.NotEmpty(t, image, "function image is required for deploy")

	cpu := strings.TrimSpace(opts.CPULimit)
	memory := strings.TrimSpace(opts.MemoryLimit)

	if fn.Limits != nil {
		if cpu == "" {
			cpu = strings.TrimSpace(fn.Limits.CPU)
		}
		if memory == "" {
			memory = strings.TrimSpace(fn.Limits.Memory)
		}
	}

	args := []string{
		"deploy",
		"--gateway", gateway,
		"--name", functionName,
		"--image", image,
	}

	if cpu != "" {
		args = append(args, "--cpu-limit", cpu)
	}
	if memory != "" {
		args = append(args, "--memory-limit", memory)
	}
	t.Logf("deploying function with: faas-cli %s", strings.Join(args, " "))
	MustCommand(t, 10*time.Minute, RepoRoot(t), nil, "faas-cli", args...)
}

func RemoveFunction(t *testing.T, functionName string, gateway string) {
	t.Helper()
	t.Logf("attempting to remove function %q with: faas-cli remove %s --gateway %s", functionName, functionName, gateway)
	_, err := TryCommand(60*time.Second, "", "faas-cli", "remove", functionName, "--gateway", gateway)
	if err != nil {
		t.Logf("cleanup remove failed for function %q: %v", functionName, err)
	}
}

func InvokeFunction(t *testing.T, baseURL string, auth GatewayAuth, functionName string, payload io.Reader) (int, []byte) {
	t.Helper()

	url := strings.TrimRight(baseURL, "/") + "/function/" + functionName
	req, err := http.NewRequest(http.MethodPost, url, payload)
	require.NoError(t, err)
	req.SetBasicAuth(auth.User, auth.Password)
	req.Header.Set("Content-Type", "text/plain")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, body
}

func InvokeFunctionEventually(t *testing.T, baseURL string, auth GatewayAuth, functionName string, payload []byte, expectedStatus int, timeout time.Duration) (int, []byte) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastStatus int
	var lastBody []byte

	for time.Now().Before(deadline) {
		status, body := InvokeFunction(t, baseURL, auth, functionName, bytes.NewReader(payload))
		lastStatus = status
		lastBody = body
		if status == expectedStatus {
			return status, body
		}
		time.Sleep(700 * time.Millisecond)
	}

	t.Fatalf("invoke %q did not reach status %d within %s, last status=%d, body=%s", functionName, expectedStatus, timeout, lastStatus, string(lastBody))
	return 0, nil
}

func ListFunctions(t *testing.T, baseURL string, auth GatewayAuth) []FunctionStatus {
	t.Helper()

	url := strings.TrimRight(baseURL, "/") + "/system/functions"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	req.SetBasicAuth(auth.User, auth.Password)

	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "list functions failed: %s", string(body))

	out := make([]FunctionStatus, 0)
	require.NoError(t, json.Unmarshal(body, &out), "invalid /system/functions payload: %s", string(body))
	return out
}

func GetFunction(t *testing.T, baseURL string, auth GatewayAuth, functionName string) FunctionStatus {
	t.Helper()
	for _, fn := range ListFunctions(t, baseURL, auth) {
		if fn.Name == functionName {
			return fn
		}
	}
	t.Fatalf("function %q not found in /system/functions", functionName)
	return FunctionStatus{}
}

func WaitForFunction(t *testing.T, baseURL string, auth GatewayAuth, functionName string, timeout time.Duration) FunctionStatus {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, fn := range ListFunctions(t, baseURL, auth) {
			if fn.Name == functionName {
				return fn
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("function %q not found in /system/functions within %s", functionName, timeout)
	return FunctionStatus{}
}

func WaitForContainerInfo(t *testing.T, functionName string, timeout time.Duration) ContainerInfo {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out := VagrantSSH(t, vagrantVMName, "sudo ctr -n openfaas-fn c info "+functionName+" 2>/dev/null || true")
		trimmed := strings.TrimSpace(out)
		if trimmed == "" {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var info ContainerInfo
		if err := json.Unmarshal([]byte(trimmed), &info); err == nil && info.ID == functionName {
			return info
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("container info for %q not found in openfaas-fn namespace within %s", functionName, timeout)
	return ContainerInfo{}
}

func MemoryBytes(t *testing.T, memory string) int64 {
	t.Helper()
	q, err := resource.ParseQuantity(memory)
	require.NoError(t, err)
	return q.Value()
}

func CPUNano(t *testing.T, cpu string) int64 {
	t.Helper()
	q, err := resource.ParseQuantity(cpu)
	require.NoError(t, err)
	return q.MilliValue() * 1_000_000
}

func CPUQuotaFromNano(nano int64, period uint64) int64 {
	periodInt64 := int64(period)
	quota := (nano/1_000_000_000)*periodInt64 + ((nano%1_000_000_000)*periodInt64)/1_000_000_000
	if quota < 1 {
		return 1
	}
	return quota
}
