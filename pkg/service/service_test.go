package service

import "testing"

func Test_makePlainHTTPMatcher(t *testing.T) {
	matcher := makePlainHTTPMatcher("registry.local")

	tests := []struct {
		name string
		host string
		want bool
	}{
		{name: "localhost default", host: "localhost:5000", want: true},
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

func Test_resolveDevRegistryHost(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		alias     string
		port      string
		gatewayIP string
		wantHost  string
		wantOK    bool
	}{
		{
			name:      "default port fallback",
			namespace: "registry.local",
			alias:     "registry.local",
			port:      "5050",
			gatewayIP: "10.0.2.2",
			wantHost:  "10.0.2.2:5050",
			wantOK:    true,
		},
		{
			name:      "namespace port wins",
			namespace: "registry.local:5443",
			alias:     "registry.local",
			port:      "5050",
			gatewayIP: "10.0.2.2",
			wantHost:  "10.0.2.2:5443",
			wantOK:    true,
		},
		{
			name:      "other namespace",
			namespace: "ghcr.io",
			alias:     "registry.local",
			port:      "5050",
			gatewayIP: "10.0.2.2",
			wantHost:  "",
			wantOK:    false,
		},
		{
			name:      "missing gateway ip",
			namespace: "registry.local:5050",
			alias:     "registry.local",
			port:      "5050",
			gatewayIP: "",
			wantHost:  "",
			wantOK:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotHost, gotOK := resolveDevRegistryHost(tc.namespace, tc.alias, tc.port, tc.gatewayIP)
			if gotOK != tc.wantOK {
				t.Fatalf("want ok %v, got %v", tc.wantOK, gotOK)
			}
			if gotHost != tc.wantHost {
				t.Fatalf("want host %q, got %q", tc.wantHost, gotHost)
			}
		})
	}
}

func Test_splitHostAndPort(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantHost string
		wantPort string
	}{
		{name: "plain host", input: "registry.local", wantHost: "registry.local", wantPort: ""},
		{name: "host and port", input: "registry.local:5050", wantHost: "registry.local", wantPort: "5050"},
		{name: "http host and port", input: "http://registry.local:5050/", wantHost: "registry.local", wantPort: "5050"},
		{name: "https host", input: "https://GHCR.IO/", wantHost: "ghcr.io", wantPort: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotHost, gotPort := splitHostAndPort(tc.input)
			if gotHost != tc.wantHost || gotPort != tc.wantPort {
				t.Fatalf("want (%q,%q), got (%q,%q)", tc.wantHost, tc.wantPort, gotHost, gotPort)
			}
		})
	}
}
