package service

import "testing"

func Test_parsePlainHTTPRegistries(t *testing.T) {
	t.Setenv(plainHTTPRegistriesEnvVar, " registry.local ,https://registry.local:5050/,http://LOCALHOST:5000, ,")

	hosts := parsePlainHTTPRegistries()

	want := []string{
		"registry.local",
		"registry.local:5050",
		"localhost:5000",
	}

	for _, host := range want {
		if _, ok := hosts[host]; !ok {
			t.Fatalf("expected host %q in parsed map, got %#v", host, hosts)
		}
	}
}

func Test_makePlainHTTPMatcher_ExtraHosts(t *testing.T) {
	matcher := makePlainHTTPMatcher(map[string]struct{}{
		"registry.local":      {},
		"registry.local:5050": {},
	})

	tests := []struct {
		name string
		host string
		want bool
	}{
		{name: "hostname only", host: "registry.local", want: true},
		{name: "hostname with port", host: "registry.local:5050", want: true},
		{name: "other registry", host: "ghcr.io", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := matcher(tc.host)
			if err != nil {
				t.Fatalf("want no error, got %v", err)
			}

			if got != tc.want {
				t.Fatalf("host %q: want %v, got %v", tc.host, tc.want, got)
			}
		})
	}
}
