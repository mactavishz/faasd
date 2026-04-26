package handlers

import (
	"strings"

	faasd "github.com/openfaas/faasd/pkg"
)

// ParseFunctionNameNamespace parses a function reference from invoke paths.
// Accepts either <name> or <name>.<namespace>.
func ParseFunctionNameNamespace(functionName string) (string, string) {
	namespace := faasd.DefaultFunctionNamespace
	if strings.Contains(functionName, ".") {
		namespace = functionName[strings.LastIndex(functionName, ".")+1:]
	}
	actualFunctionName := functionName
	if strings.Contains(functionName, ".") {
		actualFunctionName = strings.TrimSuffix(functionName, "."+namespace)
	}
	return actualFunctionName, namespace
}
