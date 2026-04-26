package handlers

import (
	"fmt"
	"log"
	"net/url"

	"github.com/containerd/containerd"
)

const watchdogPort = 8080

type InvokeResolver struct {
	client *containerd.Client
}

func NewInvokeResolver(client *containerd.Client) *InvokeResolver {
	return &InvokeResolver{client: client}
}

// Resolve is used by the provider function proxy path behind the gateway.
// After the gateway forwards /function/* invokes to the provider, Resolve maps
// a function name (with optional namespace suffix) to a concrete watchdog URL.
func (i *InvokeResolver) Resolve(functionName string) (url.URL, error) {
	actualFunctionName := functionName
	log.Printf("Resolve: %q\n", actualFunctionName)

	actualFunctionName, namespace := ParseFunctionNameNamespace(functionName)

	function, err := GetFunction(i.client, actualFunctionName, namespace)
	if err != nil {
		return url.URL{}, fmt.Errorf("%s not found", actualFunctionName)
	}

	serviceIP := function.IP

	urlStr := fmt.Sprintf("http://%s:%d", serviceIP, watchdogPort)

	urlRes, err := url.Parse(urlStr)
	if err != nil {
		return url.URL{}, err
	}

	return *urlRes, nil
}
