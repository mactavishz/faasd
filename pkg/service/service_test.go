package service

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"runtime"
	"testing"

	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestValidateOCIImageArchive(t *testing.T) {
	tests := []struct {
		name    string
		archive []byte
		wantErr bool
	}{
		{
			name:    "host platform OCI archive",
			archive: testOCIArchive(t, []ocispec.Platform{hostPlatform()}, "faasd.local/fn:latest"),
			wantErr: false,
		},
		{
			name:    "nested multi platform OCI archive",
			archive: testNestedOCIArchive(t, []ocispec.Platform{hostPlatform(), nonHostPlatform()}, "faasd.local/fn:latest"),
			wantErr: false,
		},
		{
			name:    "non host platform OCI archive",
			archive: testOCIArchive(t, []ocispec.Platform{nonHostPlatform()}, "faasd.local/fn:latest"),
			wantErr: true,
		},
		{
			name:    "docker archive",
			archive: testDockerArchive(t),
			wantErr: true,
		},
		{
			name:    "missing image reference",
			archive: testOCIArchive(t, []ocispec.Platform{hostPlatform()}, ""),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOCIImageArchive(bytes.NewReader(tt.archive))
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateOCIImageArchive() error = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func testOCIArchive(t *testing.T, platforms []ocispec.Platform, ref string) []byte {
	t.Helper()

	index := ocispec.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		Manifests: make([]ocispec.Descriptor, 0, len(platforms)),
	}
	for _, platform := range platforms {
		p := platform
		index.Manifests = append(index.Manifests, ocispec.Descriptor{
			MediaType:   ocispec.MediaTypeImageManifest,
			Digest:      "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Size:        1,
			Platform:    &p,
			Annotations: map[string]string{ocispec.AnnotationRefName: ref},
		})
	}

	return testTar(t, map[string]any{
		ocispec.ImageLayoutFile: ocispec.ImageLayout{Version: ocispec.ImageLayoutVersion},
		"index.json":            index,
	})
}

func testNestedOCIArchive(t *testing.T, platformList []ocispec.Platform, ref string) []byte {
	t.Helper()

	nested := ocispec.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		Manifests: make([]ocispec.Descriptor, 0, len(platformList)),
	}
	for _, platform := range platformList {
		p := platform
		nested.Manifests = append(nested.Manifests, ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Digest:    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Size:      1,
			Platform:  &p,
		})
	}

	nestedData, err := json.Marshal(nested)
	if err != nil {
		t.Fatal(err)
	}

	index := ocispec.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		Manifests: []ocispec.Descriptor{
			{
				MediaType: ocispec.MediaTypeImageIndex,
				Digest:    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Size:      int64(len(nestedData)),
				Annotations: map[string]string{
					"io.containerd.image.name": ref,
				},
			},
		},
	}

	return testTar(t, map[string]any{
		ocispec.ImageLayoutFile: ocispec.ImageLayout{Version: ocispec.ImageLayoutVersion},
		"index.json":            index,
		"blobs/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": json.RawMessage(nestedData),
	})
}

func testDockerArchive(t *testing.T) []byte {
	t.Helper()
	return testTar(t, map[string]any{
		"manifest.json": []map[string]any{{"Config": "config.json", "RepoTags": []string{"fn:latest"}}},
	})
}

func hostPlatform() ocispec.Platform {
	return ocispec.Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}
}

func nonHostPlatform() ocispec.Platform {
	platform := hostPlatform()
	if platform.Architecture == "amd64" {
		platform.Architecture = "arm64"
	} else {
		platform.Architecture = "amd64"
	}
	return platform
}

func testTar(t *testing.T, files map[string]any) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, value := range files {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
