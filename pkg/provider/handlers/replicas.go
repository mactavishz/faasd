package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/containerd/containerd"
	"github.com/gorilla/mux"
)

// MakeReplicaReaderHandler handles GET /system/function/{name} on the provider.
// The gateway forwards single-function status lookups here, including scale-
// from-zero polling paths that need desired vs available replica state.
func MakeReplicaReaderHandler(client *containerd.Client) func(w http.ResponseWriter, r *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		functionName := vars["name"]
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

		if stored, ok := GetStoredFunction(lookupNamespace, functionName); ok {
			found := BuildFunctionStatus(client, stored)
			functionBytes, _ := json.Marshal(found)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(functionBytes)
			return
		}

		if f, err := GetFunction(client, functionName, lookupNamespace); err == nil {
			stored := NewStoredFunctionFromRuntime(f)
			getFunctionStore().Put(stored)
			found := BuildFunctionStatus(client, stored)
			functionBytes, _ := json.Marshal(found)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(functionBytes)
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}
}
