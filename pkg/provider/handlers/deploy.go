package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	gocni "github.com/containerd/go-cni"
	"github.com/distribution/reference"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/openfaas/faas-provider/types"
	cninetwork "github.com/openfaas/faasd/pkg/cninetwork"
	"github.com/openfaas/faasd/pkg/service"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	annotationLabelPrefix          = "com.openfaas.annotations."
	defaultCPUCFSPeriodMicrosecond = uint64(100000)
	defaultGatewayURL              = "http://faasd.com:8080"
	faasdGatewayURLEnv             = "FAASD_GATEWAY_URL"
)

// MakeDeployHandler handles POST /system/functions on the faasd provider.
// The gateway forwards deploy requests here, and this handler validates input,
// creates function runtime resources, then stores metadata for status/scaling.
func MakeDeployHandler(client *containerd.Client, cni gocni.CNI, secretMountPath string, alwaysPull bool, autoScalerController *FaasdAutoScalerController, callGraphController *FaasdCallGraphController) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		if r.Body == nil {
			http.Error(w, "expected a body", http.StatusBadRequest)
			return
		}

		defer r.Body.Close()

		req, imageArchive, closeArchive, err := readFunctionDeploymentRequest(r)
		if closeArchive != nil {
			defer closeArchive()
		}
		if err != nil {
			slog.Info(fmt.Sprintf("[Deploy] - error parsing input: %s", err))
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		namespace := getRequestNamespace(req.Namespace)

		// Check if namespace exists, and it has the openfaas label
		valid, err := validNamespace(client.NamespaceService(), namespace)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if !valid {
			http.Error(w, "namespace not valid", http.StatusBadRequest)
			return
		}

		namespaceSecretMountPath := getNamespaceSecretMountPath(secretMountPath, namespace)
		err = validateSecrets(namespaceSecretMountPath, req.Secrets)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		name := req.Service
		start := time.Now()
		ctx := namespaces.WithNamespace(context.Background(), namespace)

		if err := preDeploy(client, 1); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			slog.Info(fmt.Sprintf("[Deploy] error deploying %s, error: %s\n", name, err))
			return
		}

		if err := deploy(ctx, req, client, cni, namespaceSecretMountPath, alwaysPull, imageArchive); err != nil {
			slog.Info(fmt.Sprintf("[Deploy] error deploying %s, error: %s\n", name, err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		stored := PutFunctionFromDeployment(req, namespace)
		RegisterCallGraphFunction(client, namespace, name, stored.Labels)
		if callGraphController != nil {
			callGraphController.recordScaleUp(name, time.Since(start), true)
		}
		if autoScalerController != nil {
			autoScalerController.RegisterFunctionWithState(namespace, name, ensureFunctionLabelsForAutoscaler(stored.Labels), autoscaler.StateActive)
		}
	}
}

func readFunctionDeploymentRequest(r *http.Request) (types.FunctionDeployment, multipart.File, func(), error) {
	contentType := strings.ToLower(r.Header.Get("Content-Type"))
	if strings.HasPrefix(contentType, "multipart/form-data") {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			return types.FunctionDeployment{}, nil, nil, err
		}

		raw := r.FormValue("deployment")
		if strings.TrimSpace(raw) == "" {
			return types.FunctionDeployment{}, nil, nil, fmt.Errorf("multipart deployment field is required")
		}

		req := types.FunctionDeployment{}
		if err := json.Unmarshal([]byte(raw), &req); err != nil {
			return types.FunctionDeployment{}, nil, nil, err
		}

		file, _, err := r.FormFile("image")
		if err != nil {
			return types.FunctionDeployment{}, nil, nil, fmt.Errorf("multipart image file is required: %w", err)
		}

		return req, file, func() {
			_ = file.Close()
			if r.MultipartForm != nil {
				_ = r.MultipartForm.RemoveAll()
			}
		}, nil
	}

	body, _ := io.ReadAll(r.Body)
	req := types.FunctionDeployment{}
	if err := json.Unmarshal(body, &req); err != nil {
		return types.FunctionDeployment{}, nil, nil, err
	}
	return req, nil, nil, nil
}

// prepull is an optimization which means an image can be pulled before a deployment
// request, since a deployment request first deletes the active function before
// trying to deploy a new one.
func prepull(ctx context.Context, req types.FunctionDeployment, client *containerd.Client, alwaysPull bool, imageArchive io.ReadSeeker) (containerd.Image, error) {
	start := time.Now()
	snapshotter := ""
	if val, ok := os.LookupEnv("snapshotter"); ok {
		snapshotter = val
	}

	if imageArchive != nil {
		image, err := service.PrepareOCIImageArchive(ctx, client, imageArchive, req.Image, snapshotter)
		if err != nil {
			return nil, err
		}
		size, _ := image.Size(ctx)
		slog.Info(fmt.Sprintf("Imported OCI image archive for: %s size: %d, took: %fs\n", image.Name(), size, time.Since(start).Seconds()))
		return image, nil
	}

	r, err := reference.ParseNormalizedNamed(req.Image)
	if err != nil {
		return nil, err
	}

	imgRef := reference.TagNameOnly(r).String()

	image, err := service.PrepareImage(ctx, client, imgRef, snapshotter, alwaysPull)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to pull image %s", imgRef)
	}

	size, _ := image.Size(ctx)
	slog.Info(fmt.Sprintf("Image for: %s size: %d, took: %fs\n", image.Name(), size, time.Since(start).Seconds()))

	return image, nil
}

func deploy(ctx context.Context, req types.FunctionDeployment, client *containerd.Client, cni gocni.CNI, secretMountPath string, alwaysPull bool, imageArchive io.ReadSeeker) error {

	snapshotter := ""
	if val, ok := os.LookupEnv("snapshotter"); ok {
		snapshotter = val
	}

	image, err := prepull(ctx, req, client, alwaysPull, imageArchive)
	if err != nil {
		return err
	}

	envs := prepareEnv(req.EnvProcess, req.EnvVars)
	mounts := getOSMounts()

	for _, secret := range req.Secrets {
		mounts = append(mounts, specs.Mount{
			Destination: path.Join("/var/openfaas/secrets", secret),
			Type:        "bind",
			Source:      path.Join(secretMountPath, secret),
			Options:     []string{"rbind", "ro"},
		})
	}

	name := req.Service

	labels, err := buildLabels(&req)
	if err != nil {
		return fmt.Errorf("unable to apply labels to container: %s, error: %w", name, err)
	}

	var memory *specs.LinuxMemory
	if req.Limits != nil && len(req.Limits.Memory) > 0 {
		memory = &specs.LinuxMemory{}

		qty, err := resource.ParseQuantity(req.Limits.Memory)
		if err != nil {
			slog.Info(fmt.Sprintf("error parsing (%q) as quantity: %s", req.Limits.Memory, err.Error()))
		}
		v := qty.Value()
		memory.Limit = &v
	}

	var cpu *specs.LinuxCPU
	if req.Limits != nil && len(strings.TrimSpace(req.Limits.CPU)) > 0 {
		cpu, err = buildCPULimit(req.Limits.CPU)
		if err != nil {
			slog.Info(fmt.Sprintf("error parsing (%q) as CPU limit: %s", req.Limits.CPU, err.Error()))
		}
	}

	container, err := client.NewContainer(
		ctx,
		name,
		containerd.WithImage(image),
		containerd.WithSnapshotter(snapshotter),
		containerd.WithNewSnapshot(name+"-snapshot", image),
		containerd.WithNewSpec(oci.WithImageConfig(image),
			oci.WithHostname(name),
			oci.WithCapabilities([]string{"CAP_NET_RAW"}),
			oci.WithMounts(mounts),
			oci.WithEnv(envs),
			withMemory(memory),
			withCPU(cpu)),
		containerd.WithContainerLabels(labels),
	)

	if err != nil {
		return fmt.Errorf("unable to create container: %s, error: %w", name, err)
	}

	startInfo, err := createTask(ctx, container, cni)
	if err != nil {
		return err
	}

	namespace := getRequestNamespace(req.Namespace)

	slog.Info(fmt.Sprintf("[Ready] waiting for function %s.%s", name, namespace))
	if err := waitForFunctionReady(startInfo, name, namespace, functionReadyTimeout); err != nil {
		return err
	}

	return nil

}

// countFunctions returns the number of functions deployed along with a map with a count
// in each namespace
func countFunctions(client *containerd.Client) (int64, int64, error) {
	count := int64(0)
	namespaceCount := int64(0)

	namespaces := ListNamespaces(client)

	for _, namespace := range namespaces {
		fns, err := ListFunctions(client, namespace)
		if err != nil {
			return 0, 0, err
		}
		namespaceCount++
		count += int64(len(fns))
	}

	return count, namespaceCount, nil
}

func buildLabels(request *types.FunctionDeployment) (map[string]string, error) {
	labels := map[string]string{}

	if request.Labels != nil {
		for k, v := range *request.Labels {
			labels[k] = v
		}
	}

	if request.Annotations != nil {
		for k, v := range *request.Annotations {
			key := fmt.Sprintf("%s%s", annotationLabelPrefix, k)
			if _, ok := labels[key]; !ok {
				labels[key] = v
			} else {
				return nil, errors.New(fmt.Sprintf("Key %s cannot be used as a label due to a conflict with annotation prefix %s", k, annotationLabelPrefix))
			}
		}
	}

	return labels, nil
}

type functionStartInfo struct {
	ctx  context.Context
	task containerd.Task
	addr string
}

func createTask(ctx context.Context, container containerd.Container, cni gocni.CNI) (functionStartInfo, error) {

	name := container.ID()

	task, taskErr := container.NewTask(ctx, cio.BinaryIO("/usr/local/bin/faasd", nil))

	if taskErr != nil {
		return functionStartInfo{}, fmt.Errorf("unable to start task: %s, error: %w", name, taskErr)
	}

	slog.Info(fmt.Sprintf("Container ID: %s\tTask ID: %s\tTask PID: %d\t\n", name, task.ID(), task.Pid()))

	labels := map[string]string{}
	_, err := cninetwork.CreateCNINetwork(ctx, cni, task, labels)

	if err != nil {
		return functionStartInfo{}, err
	}

	ip, err := cninetwork.GetIPAddress(name, task.Pid())
	if err != nil {
		return functionStartInfo{}, err
	}

	slog.Info(fmt.Sprintf("%s has IP: %s.\n", name, ip))

	if _, err := task.Wait(ctx); err != nil {
		return functionStartInfo{}, errors.Wrapf(err, "Unable to wait for task to start: %s", name)
	}

	if startErr := task.Start(ctx); startErr != nil {
		return functionStartInfo{}, errors.Wrapf(startErr, "Unable to start task: %s", name)
	}

	return functionStartInfo{ctx: ctx, task: task, addr: fmt.Sprintf("%s:%d", ip, watchdogPort)}, nil
}

func prepareEnv(envProcess string, reqEnvVars map[string]string) []string {
	envs := []string{}
	fprocessFound := false
	faasdGatewayURLFound := false
	fprocess := "fprocess=" + envProcess
	if len(envProcess) > 0 {
		fprocessFound = true
	}

	for k, v := range reqEnvVars {
		if k == "fprocess" {
			fprocessFound = true
			fprocess = v
		} else if k == faasdGatewayURLEnv {
			faasdGatewayURLFound = true
			envs = append(envs, k+"="+strings.TrimRight(v, "/"))
		} else {
			envs = append(envs, k+"="+v)
		}
	}

	if !faasdGatewayURLFound {
		envs = append(envs, faasdGatewayURLEnv+"="+getFaasdGatewayURL())
	}

	if fprocessFound {
		envs = append(envs, fprocess)
	}
	return envs
}

func getFaasdGatewayURL() string {
	value := strings.TrimSpace(os.Getenv(faasdGatewayURLEnv))
	if len(value) == 0 {
		return defaultGatewayURL
	}

	return strings.TrimRight(value, "/")
}

// getOSMounts provides a mount for os-specific files such
// as the hosts file and resolv.conf
func getOSMounts() []specs.Mount {
	// Prior to hosts_dir env-var, this value was set to
	// os.Getwd()
	hostsDir := "/var/lib/faasd"
	if v, ok := os.LookupEnv("hosts_dir"); ok && len(v) > 0 {
		hostsDir = v
	}

	mounts := []specs.Mount{}
	mounts = append(mounts, specs.Mount{
		Destination: "/etc/resolv.conf",
		Type:        "bind",
		Source:      path.Join(hostsDir, "resolv.conf"),
		Options:     []string{"rbind", "ro"},
	})

	mounts = append(mounts, specs.Mount{
		Destination: "/etc/hosts",
		Type:        "bind",
		Source:      path.Join(hostsDir, "hosts"),
		Options:     []string{"rbind", "ro"},
	})
	return mounts
}

func validateSecrets(secretMountPath string, secrets []string) error {
	for _, secret := range secrets {
		if _, err := os.Stat(path.Join(secretMountPath, secret)); err != nil {
			return fmt.Errorf("unable to find secret: %s", secret)
		}
	}
	return nil
}

func buildCPULimit(value string) (*specs.LinuxCPU, error) {
	nano, err := parseCPUNano(value)
	if err != nil {
		return nil, err
	}

	if nano <= 0 {
		return nil, nil
	}

	period := defaultCPUCFSPeriodMicrosecond
	periodInt64 := int64(period)

	// Convert NanoCPUs to CFS quota/period used by OCI runtimes.
	quota := (nano/1_000_000_000)*periodInt64 + ((nano%1_000_000_000)*periodInt64)/1_000_000_000
	if quota < 1 {
		quota = 1
	}

	return &specs.LinuxCPU{
		Quota:  &quota,
		Period: &period,
	}, nil
}

func parseCPUNano(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("cpu is empty")
	}

	if milliStr, ok := strings.CutSuffix(s, "m"); ok {
		r, ok := new(big.Rat).SetString(milliStr)
		if !ok {
			return 0, fmt.Errorf("invalid cpu value: %q", s)
		}

		r.Mul(r, big.NewRat(1_000_000, 1))
		return ratToInt64(r, "cpu")
	}

	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return 0, fmt.Errorf("invalid cpu value: %q", s)
	}

	r.Mul(r, big.NewRat(1_000_000_000, 1))
	return ratToInt64(r, "cpu")
}

func ratToInt64(r *big.Rat, field string) (int64, error) {
	if r.Sign() < 0 {
		return 0, fmt.Errorf("%s must be non-negative", field)
	}

	i := new(big.Int).Quo(r.Num(), r.Denom())
	if !i.IsInt64() {
		return 0, fmt.Errorf("%s value overflows int64", field)
	}

	return i.Int64(), nil
}

func withMemory(mem *specs.LinuxMemory) oci.SpecOpts {
	return func(ctx context.Context, _ oci.Client, c *containers.Container, s *oci.Spec) error {
		if mem != nil {
			if s.Linux == nil {
				s.Linux = &specs.Linux{}
			}
			if s.Linux.Resources == nil {
				s.Linux.Resources = &specs.LinuxResources{}
			}
			if s.Linux.Resources.Memory == nil {
				s.Linux.Resources.Memory = &specs.LinuxMemory{}
			}
			s.Linux.Resources.Memory.Limit = mem.Limit
		}
		return nil
	}
}

func withCPU(cpu *specs.LinuxCPU) oci.SpecOpts {
	return func(ctx context.Context, _ oci.Client, c *containers.Container, s *oci.Spec) error {
		if cpu != nil {
			if s.Linux == nil {
				s.Linux = &specs.Linux{}
			}
			if s.Linux.Resources == nil {
				s.Linux.Resources = &specs.LinuxResources{}
			}
			if s.Linux.Resources.CPU == nil {
				s.Linux.Resources.CPU = &specs.LinuxCPU{}
			}

			s.Linux.Resources.CPU.Quota = cpu.Quota
			s.Linux.Resources.CPU.Period = cpu.Period
		}

		return nil
	}
}

func preDeploy(client *containerd.Client, additional int64) error {
	count, countNs, err := countFunctions(client)
	slog.Info(fmt.Sprintf("Function count: %d, Namespace count: %d\n", count, countNs))
	return err
}
