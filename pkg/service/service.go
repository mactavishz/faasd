package service

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/docker/cli/cli/config"
	"github.com/docker/cli/cli/config/configfile"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

// dockerConfigDir contains "config.json"
const dockerConfigDir = "/var/lib/faasd/.docker/"

const containerdImageNameAnnotation = "io.containerd.image.name"

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
	registryOpts := []docker.RegistryOpt{
		docker.WithPlainHTTP(docker.MatchLocalhost),
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

	opts := docker.ResolverOptions{
		Hosts: docker.ConfigureDefaultRegistries(registryOpts...),
	}
	return docker.NewResolver(opts), nil
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

func PrepareOCIImageArchive(ctx context.Context, client *containerd.Client, archive io.ReadSeeker, imageName, snapshotter string) (containerd.Image, error) {
	if err := validateOCIImageArchive(archive); err != nil {
		return nil, err
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	if _, err := client.Import(ctx, archive, containerd.WithAllPlatforms(true), containerd.WithIndexName(imageName)); err != nil {
		return nil, fmt.Errorf("cannot import OCI image archive: %w", err)
	}

	image, err := client.GetImage(ctx, imageName)
	if err != nil {
		return nil, err
	}

	unpacked, err := image.IsUnpacked(ctx, snapshotter)
	if err != nil {
		return nil, fmt.Errorf("cannot check if unpacked: %s", err)
	}
	if !unpacked {
		if err := image.Unpack(ctx, snapshotter); err != nil {
			return nil, fmt.Errorf("cannot unpack: %s", err)
		}
	}

	return image, nil
}

func PrepareLocalImage(ctx context.Context, client *containerd.Client, imageName, snapshotter string) (containerd.Image, error) {
	image, err := client.GetImage(ctx, imageName)
	if err != nil {
		return nil, fmt.Errorf("archive-backed image %q is missing from local containerd store: %w", imageName, err)
	}

	unpacked, err := image.IsUnpacked(ctx, snapshotter)
	if err != nil {
		return nil, fmt.Errorf("cannot check if unpacked: %s", err)
	}
	if !unpacked {
		if err := image.Unpack(ctx, snapshotter); err != nil {
			return nil, fmt.Errorf("cannot unpack: %s", err)
		}
	}

	return image, nil
}

func validateOCIImageArchive(archive io.ReadSeeker) error {
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return err
	}

	tmpDir, err := os.MkdirTemp("", "faasd-oci-archive-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	if err := extractOCIArchive(archive, tmpDir); err != nil {
		return err
	}

	index, err := layout.ImageIndexFromPath(tmpDir)
	if err != nil {
		return fmt.Errorf("unsupported image archive: expected OCI layout with oci-layout and index.json: %w", err)
	}

	result, err := inspectOCIImageIndex(index)
	if err != nil {
		return err
	}
	if !result.hostMatch {
		return fmt.Errorf("OCI image archive does not contain a manifest for host platform %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	if len(result.refs) == 0 {
		return fmt.Errorf("OCI image archive does not contain an image reference")
	}
	if len(result.refs) > 1 {
		return fmt.Errorf("OCI image archive contains multiple image references")
	}

	return nil
}

type ociArchiveInspection struct {
	refs      map[string]struct{}
	hostMatch bool
}

func extractOCIArchive(archive io.Reader, dest string) error {
	tr := tar.NewReader(archive)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read OCI archive: %w", err)
		}

		name := strings.TrimPrefix(filepath.Clean(hdr.Name), string(filepath.Separator))
		name = strings.TrimPrefix(name, "./")
		if name == "." || name == "" {
			continue
		}
		if filepath.IsAbs(hdr.Name) || strings.HasPrefix(name, "../") || name == ".." {
			return fmt.Errorf("OCI image archive contains unsafe path %q", hdr.Name)
		}
		if name == "manifest.json" {
			return fmt.Errorf("Docker image archives are not supported; provide an OCI image archive")
		}

		target := filepath.Join(dest, name)
		if !strings.HasPrefix(target, filepath.Clean(dest)+string(filepath.Separator)) && target != filepath.Clean(dest) {
			return fmt.Errorf("OCI image archive contains unsafe path %q", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(file, tr)
			closeErr := file.Close()
			if copyErr != nil {
				return fmt.Errorf("extract OCI archive file %s: %w", name, copyErr)
			}
			if closeErr != nil {
				return closeErr
			}
		case tar.TypeSymlink, tar.TypeLink:
			return fmt.Errorf("OCI image archive contains unsupported link %q", hdr.Name)
		default:
			return fmt.Errorf("OCI image archive contains unsupported entry %q", hdr.Name)
		}
	}
	return nil
}

func inspectOCIImageIndex(index v1.ImageIndex) (ociArchiveInspection, error) {
	result := ociArchiveInspection{refs: map[string]struct{}{}}
	if err := result.walkIndex(index); err != nil {
		return result, err
	}
	return result, nil
}

func (i *ociArchiveInspection) walkIndex(index v1.ImageIndex) error {
	manifest, err := index.IndexManifest()
	if err != nil {
		return fmt.Errorf("read OCI index: %w", err)
	}

	for _, desc := range manifest.Manifests {
		if ref := imageRefFromAnnotations(desc.Annotations); ref != "" {
			i.refs[ref] = struct{}{}
		}

		if desc.MediaType.IsImage() {
			if isRealPlatform(desc.Platform) && desc.Platform.Satisfies(v1.Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}) {
				i.hostMatch = true
			}
			continue
		}

		if !desc.MediaType.IsIndex() {
			continue
		}

		nested, err := index.ImageIndex(desc.Digest)
		if err != nil {
			return fmt.Errorf("read nested OCI index %s: %w", desc.Digest, err)
		}
		if err := i.walkIndex(nested); err != nil {
			return err
		}
	}
	return nil
}

func isRealPlatform(platform *v1.Platform) bool {
	return platform != nil && platform.OS != "" && platform.Architecture != "" && platform.OS != "unknown" && platform.Architecture != "unknown"
}

func imageRefFromAnnotations(annotations map[string]string) string {
	if annotations == nil {
		return ""
	}
	if ref := strings.TrimSpace(annotations[containerdImageNameAnnotation]); ref != "" {
		return ref
	}
	if ref := strings.TrimSpace(annotations[ocispec.AnnotationRefName]); ref != "" {
		return ref
	}
	return ""
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
