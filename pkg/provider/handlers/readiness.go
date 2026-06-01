package handlers

import (
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	"github.com/containerd/containerd"
)

const (
	functionReadyTimeout        = 10 * time.Second
	functionReadyRequestTimeout = 100 * time.Millisecond
	functionReadyInitialDelay   = 50 * time.Millisecond
	functionReadyMaxDelay       = 250 * time.Millisecond
)

func waitForFunctionReady(startInfo functionStartInfo, functionName, namespace string, timeout time.Duration) error {
	start := time.Now()
	deadline := start.Add(timeout)
	attempt := 0
	var lastErr error

	for {
		err := probeFunctionReady(startInfo.addr)

		if err == nil {
			slog.Info(fmt.Sprintf("[Ready] function %s.%s ready in %s", functionName, namespace, time.Since(start)))
			return nil
		}

		lastErr = err

		if taskErr := functionTaskTerminalError(startInfo, functionName, namespace); taskErr != nil {
			return taskErr
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("function %s.%s not ready within %s: %w", functionName, namespace, timeout, lastErr)
		}

		delay := readinessBackoffDelay(attempt)
		attempt++
		remaining := time.Until(deadline)
		if delay > remaining {
			delay = remaining
		}

		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

func functionTaskTerminalError(startInfo functionStartInfo, functionName, namespace string) error {
	status, err := startInfo.task.Status(startInfo.ctx)
	if err != nil {
		return fmt.Errorf("cannot inspect task status for %s.%s while waiting for readiness: %w", functionName, namespace, err)
	}

	if status.Status == containerd.Stopped || status.Status == containerd.Paused || status.Status == containerd.Unknown {
		return fmt.Errorf("function %s.%s task entered %q while waiting for readiness", functionName, namespace, status.Status)
	}

	return nil
}

func probeFunctionReady(addr string) error {
	httpClient := http.Client{Timeout: functionReadyRequestTimeout}
	resp, err := httpClient.Get("http://" + addr + "/_/ready")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ready endpoint returned %s", resp.Status)
	}

	return nil
}

func readinessBackoffDelay(attempt int) time.Duration {
	delay := functionReadyInitialDelay
	for i := 0; i < attempt && delay < functionReadyMaxDelay; i++ {
		delay *= 2
	}

	if delay > functionReadyMaxDelay {
		delay = functionReadyMaxDelay
	}

	if delay <= 0 {
		return 0
	}

	return time.Duration(rand.Int63n(int64(delay)))
}
