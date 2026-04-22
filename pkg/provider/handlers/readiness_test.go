package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func Test_probeFunctionReady_OK(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_/ready" {
			t.Fatalf("expected /_/ready path, got %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	if err := probeFunctionReady(ts.Listener.Addr().String()); err != nil {
		t.Fatalf("want no error, got %v", err)
	}
}

func Test_probeFunctionReady_Non200(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	err := probeFunctionReady(ts.Listener.Addr().String())
	if err == nil {
		t.Fatalf("want error, got nil")
	}
}

func Test_probeFunctionReady_ConnectionError(t *testing.T) {
	listener := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	addr := listener.Listener.Addr().String()
	listener.Close()

	err := probeFunctionReady(addr)
	if err == nil {
		t.Fatalf("want error, got nil")
	}
}

func Test_readinessBackoffDelay_Bounded(t *testing.T) {
	for i := 0; i < 50; i++ {
		d := readinessBackoffDelay(i)
		if d < 0 {
			t.Fatalf("delay should be non-negative, got %s", d)
		}
		if d > functionReadyMaxDelay {
			t.Fatalf("delay should be <= %s, got %s", functionReadyMaxDelay, d)
		}
	}
}

func Test_probeFunctionReady_SucceedsAfterRetries(t *testing.T) {
	var attempts int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	deadline := time.Now().Add(2 * time.Second)
	try := 0
	var lastErr error

	for {
		err := probeFunctionReady(ts.Listener.Addr().String())
		if err == nil {
			if atomic.LoadInt32(&attempts) < 3 {
				t.Fatalf("expected at least 3 attempts, got %d", attempts)
			}
			return
		}

		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("did not succeed before deadline: %v", lastErr)
		}

		d := readinessBackoffDelay(try)
		try++
		time.Sleep(d)
	}
}

func Test_readinessBackoffDelay_ExponentialCeiling(t *testing.T) {
	for i := 0; i < 16; i++ {
		d := readinessBackoffDelay(i)
		if d < 0 || d > functionReadyMaxDelay {
			t.Fatalf("attempt %d produced invalid delay %s", i, d)
		}
	}
}

func Test_probeFunctionReady_ErrorMessageIncludesStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGatewayTimeout)
	}))
	defer ts.Close()

	err := probeFunctionReady(ts.Listener.Addr().String())
	if err == nil {
		t.Fatalf("want error, got nil")
	}

	if got, want := err.Error(), "504 Gateway Timeout"; !strings.Contains(got, want) {
		t.Fatalf("want error to contain %q, got %q", want, got)
	}
}
