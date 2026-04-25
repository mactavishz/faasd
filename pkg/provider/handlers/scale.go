package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/namespaces"
	gocni "github.com/containerd/go-cni"

	"github.com/openfaas/faas-provider/types"
	"github.com/openfaas/faasd/pkg"
)

// MakeReplicaUpdateHandler handles POST /system/scale-function/{name} on the provider.
// The gateway scaler forwards scale-up requests here when available replicas are 0,
// and this handler applies scale-down/scale-up against provider runtime state.
func MakeReplicaUpdateHandler(client *containerd.Client, cni gocni.CNI, controller *FaasdAutoScaler) func(w http.ResponseWriter, r *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {

		if r.Body == nil {
			http.Error(w, "expected a body", http.StatusBadRequest)
			return
		}

		defer r.Body.Close()

		body, _ := io.ReadAll(r.Body)

		req := types.ScaleServiceRequest{}
		if err := json.Unmarshal(body, &req); err != nil {
			log.Printf("[Scale] error parsing input: %s", err)
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		namespace := req.Namespace
		if namespace == "" {
			namespace = pkg.DefaultFunctionNamespace
		}

		// Check if namespace exists, and it has the openfaas label
		valid, err := validNamespace(client.NamespaceService(), namespace)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if !valid {
			http.Error(w, "namespace not valid", http.StatusBadRequest)
			return
		}

		name := req.ServiceName

		if _, exists := GetStoredFunction(namespace, name); !exists {
			if _, err := GetFunction(client, name, namespace); err != nil {
				msg := fmt.Sprintf("function: %s.%s not found", name, namespace)
				log.Printf("[Scale] %s\n", msg)
				http.Error(w, msg, http.StatusNotFound)
				return
			}
		}

		ctx := namespaces.WithNamespace(context.Background(), namespace)

		ctr, ctrErr := client.LoadContainer(ctx, name)

		var taskExists bool
		var taskStatus *containerd.Status

		var task containerd.Task
		if ctrErr == nil {
			task, taskErr := ctr.Task(ctx, nil)
			if taskErr != nil {
				taskExists = false
			} else {
				taskExists = true
				status, statusErr := task.Status(ctx)
				if statusErr != nil {
					msg := fmt.Sprintf("cannot load task status for %s, error: %s", name, statusErr)
					log.Printf("[Scale] %s\n", msg)
					http.Error(w, msg, http.StatusInternalServerError)
					return
				} else {
					taskStatus = &status
				}
			}
		}

		if req.Replicas == 0 {
			if controller == nil {
				http.Error(w, "autoscaler controller not configured", http.StatusInternalServerError)
				return
			}

			if err := controller.ScaleDown(namespace, name); err != nil {
				msg := fmt.Sprintf("cannot scale down service %s, error: %s", name, err)
				log.Printf("[Scale] %s\n", msg)
				http.Error(w, msg, http.StatusBadRequest)
				return
			}

			return
		}

		createNewTask := ctrErr != nil

		if taskExists {
			if taskStatus != nil {
				if taskStatus.Status == containerd.Paused {
					if _, err := task.Delete(ctx); err != nil {
						log.Printf("[Scale] error deleting paused task %s, error: %s\n", name, err)
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
				} else if taskStatus.Status == containerd.Stopped {
					// Stopped tasks cannot be restarted, must be removed, and created again
					if _, err := task.Delete(ctx); err != nil {
						log.Printf("[Scale] error deleting stopped task %s, error: %s\n", name, err)
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					createNewTask = true
				}
			}
		} else {
			createNewTask = true
		}

		if createNewTask {
			if controller != nil {
				if err := controller.ScaleUp(namespace, name); err != nil {
					log.Printf("[Scale] error deploying %s, error: %s\n", name, err)
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				return
			}

			startInfo, err := createTask(ctx, ctr, cni)
			if err != nil {
				log.Printf("[Scale] error deploying %s, error: %s\n", name, err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			log.Printf("[Ready] waiting for function %s.%s", name, namespace)
			if err := waitForFunctionReady(startInfo, name, namespace, functionReadyTimeout); err != nil {
				log.Printf("[Scale] readiness failed for %s, error: %s\n", name, err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
	}
}
