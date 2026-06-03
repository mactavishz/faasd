package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/namespaces"
	gocni "github.com/containerd/go-cni"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"

	"github.com/openfaas/faasd/pkg/cninetwork"
	"github.com/openfaas/faasd/pkg/service"
)

// MakeUpdateHandler handles PUT /system/functions on the faasd provider.
// The gateway forwards update requests here, and this handler redeploys the
// function runtime and refreshes stored metadata/autoscaler state.
func MakeUpdateHandler(client *containerd.Client, cni gocni.CNI, secretMountPath string, alwaysPull bool, autoScalerController *FaasdAutoScalerController, callGraphController *FaasdCallGraphController, archiveUploadTimeout time.Duration) func(w http.ResponseWriter, r *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {

		if r.Body == nil {
			http.Error(w, "expected a body", http.StatusBadRequest)
			return
		}

		extendArchiveUploadDeadlines(w, r, archiveUploadTimeout)

		defer r.Body.Close()

		req, imageArchive, closeArchive, err := readFunctionDeploymentRequest(r)
		if closeArchive != nil {
			defer closeArchive()
		}
		if err != nil {
			slog.Info(fmt.Sprintf("[Update] error parsing input: %s", err))
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		name := req.Service
		namespace := getRequestNamespace(req.Namespace)
		start := time.Now()

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

		if err := preDeploy(client, int64(0)); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			slog.Info(fmt.Sprintf("[Deploy] error deploying %s, error: %s\n", name, err))
			return
		}

		namespaceSecretMountPath := getNamespaceSecretMountPath(secretMountPath, namespace)

		function, err := GetFunction(client, name, namespace)
		if err != nil {
			msg := fmt.Sprintf("function: %s.%s not found", name, namespace)
			slog.Info(fmt.Sprintf("[Update] %s\n", msg))
			http.Error(w, msg, http.StatusNotFound)
			return
		}

		err = validateSecrets(namespaceSecretMountPath, req.Secrets)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		ctx := namespaces.WithNamespace(context.Background(), namespace)

		if _, err := prepull(ctx, req, client, alwaysPull, imageArchive); err != nil {
			slog.Info(fmt.Sprintf("[Update] error with pre-pull: %s, %s\n", name, err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		newStored := NewStoredFunctionFromDeploymentWithSource(req, namespace, imageArchive != nil)
		previousStored, hadPrevious := GetStoredFunction(namespace, name)
		if !hadPrevious {
			previousStored = NewStoredFunctionFromRuntime(function)
		}

		if callGraphController != nil {
			callGraphController.resetFunction(name)
		}

		if autoScalerController != nil && autoScalerController.Enabled() {
			store := getFunctionStore()
			store.Put(newStored)

			if err := autoScalerController.ScaleDownWhenIdle(namespace, name); err != nil {
				if strings.Contains(err.Error(), "not registered") {
					autoScalerController.RegisterFunctionWithState(namespace, name, ensureFunctionLabelsForAutoscaler(newStored.Labels), autoscaler.StateActive)
					err = autoScalerController.ScaleDownWhenIdle(namespace, name)
				}
				if err != nil {
					store.Put(previousStored)
					msg := fmt.Sprintf("cannot safely scale down function %s.%s for update: %s", name, namespace, err)
					slog.Info(fmt.Sprintf("[Update] %s\n", msg))
					http.Error(w, msg, http.StatusBadRequest)
					return
				}
			}

			if err := autoScalerController.ScaleUp(namespace, name); err != nil {
				store.Put(previousStored)
				slog.Info(fmt.Sprintf("[Update] error scaling up %s after update, error: %s\n", name, err))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			RegisterCallGraphFunction(client, namespace, name, newStored.Labels)
			return
		}

		if function.replicas != 0 {
			err = cninetwork.DeleteCNINetwork(ctx, cni, client, name)
			if err != nil {
				slog.Info(fmt.Sprintf("[Update] error removing CNI network for %s, %s\n", name, err))
			}
		}

		if err := service.Remove(ctx, client, name); err != nil {
			slog.Info(fmt.Sprintf("[Update] error removing %s, %s\n", name, err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// The pull has already been done in prepull, so we can force this pull to "false"
		pull := false

		if err := deploy(ctx, req, client, cni, namespaceSecretMountPath, pull, nil); err != nil {
			slog.Info(fmt.Sprintf("[Update] error deploying %s, error: %s\n", name, err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		PutFunctionFromDeploymentWithSource(req, namespace, imageArchive != nil)
		RegisterCallGraphFunction(client, namespace, name, newStored.Labels)
		if callGraphController != nil {
			callGraphController.recordScaleUp(name, time.Since(start), true)
		}
	}
}
