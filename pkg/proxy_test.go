package pkg

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func Test_Proxy_ToPrivateServer(t *testing.T) {

	wantBodyText := "OK"
	wantBody := []byte(wantBodyText)
	upstreamSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Body != nil {
			defer r.Body.Close()
		}

		w.WriteHeader(http.StatusOK)
		w.Write(wantBody)

	}))

	defer upstreamSvr.Close()
	u, err := url.Parse(upstreamSvr.URL)
	if err != nil {
		t.Fatalf("failed to parse upstream URL: %v", err)
	}

	port, err := getFreePort("127.0.0.1")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}

	upstreamAddr := u.Host
	proxy := NewProxy(upstreamAddr, uint32(port), "127.0.0.1", time.Second*1, &mockResolver{})
	errCh := make(chan error, 1)

	go func() {
		errCh <- proxy.Start()
	}()

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d", port), nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 200 * time.Millisecond}

	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case proxyErr := <-errCh:
			t.Fatalf("proxy exited early: %v", proxyErr)
		default:
		}

		res, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}

		resBody, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			t.Fatalf("failed to read response body: %v", readErr)
		}

		if res.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("unexpected status code %d", res.StatusCode)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if string(resBody) != string(wantBody) {
			t.Fatalf("want %s, but got %s in body", string(wantBody), string(resBody))
		}

		return
	}

	t.Fatalf("proxy did not become ready before timeout, last error: %v", lastErr)
}

func getFreePort(host string) (int, error) {
	l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

type mockResolver struct {
}

func (m *mockResolver) Start() {

}

func (m *mockResolver) Get(upstream string, got chan<- string, timeout time.Duration) {
	got <- upstream
}
