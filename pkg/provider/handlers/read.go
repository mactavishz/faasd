package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/containerd/containerd"
	"github.com/openfaas/faas-provider/types"
)

// MakeReadHandler handles GET /system/functions on the faasd provider.
// The gateway forwards list requests here, and this handler returns function
// status per namespace from store-backed metadata plus runtime availability.
func MakeReadHandler(client *containerd.Client) func(w http.ResponseWriter, r *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {

		lookupNamespace := getRequestNamespace(readNamespaceFromQuery(r))
		// Check if namespace exists, and it has the openfaas label
		valid, err := validNamespace(client.NamespaceService(), lookupNamespace)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if !valid {
			http.Error(w, "namespace not valid", http.StatusBadRequest)
			return
		}

		res := []types.FunctionStatus{}
		stored := ListStoredFunctions(lookupNamespace)
		if len(stored) == 0 {
			fns, err := ListFunctions(client, lookupNamespace)
			if err != nil {
				log.Printf("[Read] error listing functions. Error: %s", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			for _, fn := range fns {
				storedFn := NewStoredFunctionFromRuntime(*fn)
				stored = append(stored, storedFn)
				getFunctionStore().Put(storedFn)
			}
		}

		for _, fn := range stored {
			status := BuildFunctionStatus(client, fn)
			if status.Limits != nil {
				memory := resource.NewQuantity(0, resource.BinarySI)
				cpu := resource.NewScaledQuantity(0, resource.Nano)
				if status.Limits.Memory != "" {
					if parsed, err := resource.ParseQuantity(status.Limits.Memory); err == nil {
						memory = &parsed
					}
				}
				if status.Limits.CPU != "" {
					if parsed, err := resource.ParseQuantity(status.Limits.CPU); err == nil {
						cpu = &parsed
					}
				}
				limit := &types.FunctionResources{Memory: memory.String(), CPU: cpu.String()}
				if limit.Memory == "0" && limit.CPU == "0" {
					status.Limits = nil
				}
			}
			res = append(res, status)
		}

		body, _ := json.Marshal(res)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}
}
