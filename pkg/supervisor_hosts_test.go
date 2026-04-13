package pkg

import (
	"strings"
	"testing"
)

func TestRenderBaseHosts(t *testing.T) {
	hosts := string(renderBaseHosts("10.62.0.1"))

	if !strings.Contains(hosts, "127.0.0.1\tlocalhost\n") {
		t.Fatalf("base hosts missing localhost entry: %q", hosts)
	}

	if !strings.Contains(hosts, "10.62.0.1\tfaasd-provider\n") {
		t.Fatalf("base hosts missing faasd-provider entry: %q", hosts)
	}

	if !strings.Contains(hosts, "10.62.0.1\tgateway\n") {
		t.Fatalf("base hosts missing gateway entry: %q", hosts)
	}

	if !strings.Contains(hosts, "10.62.0.1\tfaasd.com\n") {
		t.Fatalf("base hosts missing faasd.com alias: %q", hosts)
	}
}

func TestAppendServiceHostsForGatewayAddsAlias(t *testing.T) {
	hosts := appendServiceHosts([]byte{}, gatewayServiceName, "10.62.0.3")
	hostsStr := string(hosts)

	if !strings.Contains(hostsStr, "10.62.0.3\tgateway\n") {
		t.Fatalf("hosts missing gateway entry: %q", hostsStr)
	}

	if !strings.Contains(hostsStr, "10.62.0.3\tfaasd.com\n") {
		t.Fatalf("hosts missing faasd.com gateway alias: %q", hostsStr)
	}
}

func TestAppendServiceHostsForNonGatewayDoesNotAddAlias(t *testing.T) {
	hosts := appendServiceHosts([]byte{}, "prometheus", "10.62.0.2")
	hostsStr := string(hosts)

	if !strings.Contains(hostsStr, "10.62.0.2\tprometheus\n") {
		t.Fatalf("hosts missing service entry: %q", hostsStr)
	}

	if strings.Contains(hostsStr, "\tfaasd.com\n") {
		t.Fatalf("non-gateway service should not add faasd.com alias: %q", hostsStr)
	}
}
