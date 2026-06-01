package service

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/docker/cli/cli/config"
	"github.com/docker/cli/cli/config/configfile"
	"golang.org/x/sys/unix"
)

// dockerConfigDir contains "config.json"
const dockerConfigDir = "/var/lib/faasd/.docker/"

const devRegistryAliasEnvVar = "FAASD_DEV_REGISTRY_ALIAS"
const devRegistryAliasDefault = "registry.local"
const devRegistryPortEnvVar = "FAASD_DEV_REGISTRY_PORT"
const devRegistryPortDefault = "5050"
const devRegistryGatewayIPEnvVar = "FAASD_DEV_REGISTRY_GATEWAY_IP"

// Remove removes a container
func Remove(ctx context.Context, client *containerd.Client, name string) error {

	container, containerErr := client.LoadContainer(ctx, name)

	if containerErr == nil {
		taskFound := true
		t, err := container.Task(ctx, nil)
		if err != nil {
			if errdefs.IsNotFound(err) {
				taskFound = false
			} else {
				return fmt.Errorf("unable to get task %w: ", err)
			}
		}

		if taskFound {
			status, err := t.Status(ctx)
			if err != nil {
				slog.Info(fmt.Sprintf("Unable to get status for: %s, error: %s", name, err.Error()))
			} else {
				slog.Info(fmt.Sprintf("Status of %s is: %s\n", name, status.Status))
			}

			var gracePeriod = time.Second * 30
			spec, err := t.Spec(ctx)
			if err == nil {
				for _, p := range spec.Process.Env {
					k, v, ok := strings.Cut(p, "=")
					if ok && k == "grace_period" {
						periodVal, err := time.ParseDuration(v)
						if err == nil {
							gracePeriod = periodVal
						}
					}
				}
			}

			if err = killTask(ctx, t, gracePeriod); err != nil {
				return fmt.Errorf("error killing task %s, %s, %w", container.ID(), name, err)
			}

		}

		if err := container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil {
			return fmt.Errorf("error deleting container %s, %s, %w", container.ID(), name, err)
		}

	} else {
		service := client.SnapshotService("")
		key := name + "-snapshot"
		if _, err := client.SnapshotService("").Stat(ctx, key); err == nil {
			service.Remove(ctx, key)
		}
	}
	return nil
}

// Adapted from Stellar - https://github.com/stellar
func killTask(ctx context.Context, task containerd.Task, gracePeriod time.Duration) error {

	wg := &sync.WaitGroup{}
	wg.Add(1)
	var err error

	waited := false
	go func() {
		defer wg.Done()
		if task != nil {
			wait, err := task.Wait(ctx)
			if err != nil {
				slog.Info(fmt.Sprintf("error waiting on task: %s", err))
				return
			}

			if err := task.Kill(ctx, unix.SIGTERM, containerd.WithKillAll); err != nil {
				slog.Info(fmt.Sprintf("error killing container task: %s", err))
			}

			select {
			case <-wait:
				waited = true
				return
			case <-time.After(gracePeriod):
				slog.Info(fmt.Sprintf("Sending SIGKILL to: %s after: %s", task.ID(), gracePeriod.Round(time.Second).String()))
				if err := task.Kill(ctx, unix.SIGKILL, containerd.WithKillAll); err != nil {
					slog.Info(fmt.Sprintf("error sending SIGKILL to task: %s", err))
				}

				return
			}
		}
	}()
	wg.Wait()

	if task != nil {
		if !waited {
			wait, err := task.Wait(ctx)
			if err != nil {
				slog.Info(fmt.Sprintf("error waiting on task after kill: %s", err))
			}

			<-wait
		}

		if _, err := task.Delete(ctx); err != nil {
			return err
		}
	}

	return err
}

func getResolver(configFile *configfile.ConfigFile) (remotes.Resolver, error) {
	devRegistryAlias := getDevRegistryAlias()
	devRegistryPort := getDevRegistryPort()
	registryOpts := []docker.RegistryOpt{
		docker.WithPlainHTTP(makePlainHTTPMatcher(devRegistryAlias)),
	}

	authOpts := []docker.AuthorizerOpt{}

	if configFile != nil {
		// credsFunc is based on https://github.com/moby/buildkit/blob/0b130cca040246d2ddf55117eeff34f546417e40/session/auth/authprovider/authprovider.go#L35
		credFunc := func(host string) (string, string, error) {
			if host == "registry-1.docker.io" {
				host = "https://index.docker.io/v1/"
			}
			ac, err := configFile.GetAuthConfig(host)
			if err != nil {
				return "", "", err
			}
			if ac.IdentityToken != "" {
				return "", ac.IdentityToken, nil
			}
			return ac.Username, ac.Password, nil
		}

		authOpts = append(authOpts, docker.WithAuthCreds(credFunc))
	}

	authorizer := docker.NewDockerAuthorizer(authOpts...)
	registryOpts = append(registryOpts, docker.WithAuthorizer(authorizer))

	defaultHosts := docker.ConfigureDefaultRegistries(registryOpts...)

	opts := docker.ResolverOptions{
		Hosts: func(namespace string) ([]docker.RegistryHost, error) {
			hosts, err := defaultHosts(namespace)
			if err != nil {
				return nil, err
			}

			if !isDevRegistryAlias(namespace, devRegistryAlias) {
				return hosts, nil
			}

			devRegistryGatewayIP, gatewayErr := detectHostGatewayIP()
			if gatewayErr != nil {
				return nil, fmt.Errorf("cannot resolve host gateway IP for %s: %w", devRegistryAlias, gatewayErr)
			}

			targetHost, shouldRewrite := resolveDevRegistryHost(namespace, devRegistryAlias, devRegistryPort, devRegistryGatewayIP)
			if !shouldRewrite {
				return hosts, nil
			}

			for i := range hosts {
				hosts[i].Host = targetHost
				hosts[i].Scheme = "http"
			}

			return hosts, nil
		},
	}
	return docker.NewResolver(opts), nil
}

func makePlainHTTPMatcher(devRegistryAlias string) func(string) (bool, error) {
	return func(host string) (bool, error) {
		match, err := docker.MatchLocalhost(host)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}

		if isDevRegistryAlias(host, devRegistryAlias) {
			return true, nil
		}

		return false, nil
	}
}

func resolveDevRegistryHost(namespace, devRegistryAlias, devRegistryPort, devRegistryGatewayIP string) (string, bool) {
	if devRegistryGatewayIP == "" {
		return "", false
	}

	hostName, port := splitHostAndPort(namespace)
	if !strings.EqualFold(hostName, devRegistryAlias) {
		return "", false
	}

	if port == "" {
		port = devRegistryPort
	}

	return net.JoinHostPort(devRegistryGatewayIP, port), true
}

func isDevRegistryAlias(host, devRegistryAlias string) bool {
	hostName, _ := splitHostAndPort(host)
	return strings.EqualFold(hostName, devRegistryAlias)
}

func splitHostAndPort(value string) (string, string) {
	host := strings.ToLower(strings.TrimSpace(value))
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimSuffix(host, "/")

	if host == "" {
		return "", ""
	}

	if strings.Contains(host, ":") {
		if parsedHost, parsedPort, err := net.SplitHostPort(host); err == nil {
			return parsedHost, parsedPort
		}
	}

	return host, ""
}

func getDevRegistryAlias() string {
	if value := strings.TrimSpace(os.Getenv(devRegistryAliasEnvVar)); value != "" {
		return strings.ToLower(value)
	}

	return devRegistryAliasDefault
}

func getDevRegistryPort() string {
	if value := strings.TrimSpace(os.Getenv(devRegistryPortEnvVar)); value != "" {
		return value
	}

	return devRegistryPortDefault
}

func detectHostGatewayIP() (string, error) {
	if value := strings.TrimSpace(os.Getenv(devRegistryGatewayIPEnvVar)); value != "" {
		return value, nil
	}

	raw, err := os.ReadFile("/proc/net/route")
	if err != nil {
		return "", err
	}

	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[1] != "00000000" {
			continue
		}

		gatewayHex := fields[2]
		parsed, parseErr := strconv.ParseUint(gatewayHex, 16, 32)
		if parseErr != nil {
			continue
		}

		ip := net.IPv4(byte(parsed), byte(parsed>>8), byte(parsed>>16), byte(parsed>>24))
		if ip.IsUnspecified() {
			continue
		}

		return ip.String(), nil
	}

	return "", fmt.Errorf("no default route found in /proc/net/route")
}

func PrepareImage(ctx context.Context, client *containerd.Client, imageName, snapshotter string, pullAlways bool) (containerd.Image, error) {
	var (
		empty      containerd.Image
		resolver   remotes.Resolver
		configFile *configfile.ConfigFile
	)

	if _, statErr := os.Stat(filepath.Join(dockerConfigDir, config.ConfigFileName)); statErr == nil {
		loadedConfig, err := config.Load(dockerConfigDir)
		if err != nil {
			return nil, err
		}
		configFile = loadedConfig
	} else if !os.IsNotExist(statErr) {
		return empty, statErr
	}

	resolver, err := getResolver(configFile)
	if err != nil {
		return empty, err
	}

	var image containerd.Image
	if pullAlways {
		img, err := pullImage(ctx, client, resolver, imageName)
		if err != nil {
			return empty, err
		}

		image = img
	} else {
		img, err := client.GetImage(ctx, imageName)
		if err != nil {
			if !errdefs.IsNotFound(err) {
				return empty, err
			}
			img, err := pullImage(ctx, client, resolver, imageName)
			if err != nil {
				return empty, err
			}
			image = img
		} else {
			image = img
		}
	}

	unpacked, err := image.IsUnpacked(ctx, snapshotter)
	if err != nil {
		return empty, fmt.Errorf("cannot check if unpacked: %s", err)
	}

	if !unpacked {
		if err := image.Unpack(ctx, snapshotter); err != nil {
			return empty, fmt.Errorf("cannot unpack: %s", err)
		}
	}

	return image, nil
}

func pullImage(ctx context.Context, client *containerd.Client, resolver remotes.Resolver, imageName string) (containerd.Image, error) {

	var empty containerd.Image

	rOpts := []containerd.RemoteOpt{
		containerd.WithPullUnpack,
	}

	if resolver != nil {
		rOpts = append(rOpts, containerd.WithResolver(resolver))
	}

	img, err := client.Pull(ctx, imageName, rOpts...)
	if err != nil {
		return empty, fmt.Errorf("cannot pull: %s", err)
	}

	return img, nil
}
