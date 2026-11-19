package cloudwatchlogs

import (
	"crypto/tls"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// TestClientTLS drives the TLS settings through the service client (AWS SDK on the Grow HTTP
// client); the generated core_tls_test.go covers the shared transport only.
func TestClientTLS(t *testing.T) {
	var gotClientCert bool
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotClientCert = len(r.TLS.PeerCertificates) > 0
		w.WriteHeader(http.StatusOK)
	}))
	ts.TLS = &tls.Config{ClientAuth: tls.RequestClientCert, MinVersion: tls.VersionTLS12}
	ts.Config.ErrorLog = log.New(io.Discard, "", 0) // the unknown-CA case logs a handshake error
	ts.StartTLS()
	defer ts.Close()
	caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ts.Certificate().Raw}))

	t.Run("unknown CA fails", func(t *testing.T) {
		c := newTestClient(t, ts.URL, func(cfg *Config) { cfg.Retry = fastRetry(0) })
		if _, err := c.Ping(t.Context()); CodeOf(err) != ErrorCodeUnavailable {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("inline CA PEM", func(t *testing.T) {
		c := newTestClient(t, ts.URL, func(cfg *Config) { cfg.TLS.CACertPEM = caPEM })
		if _, err := c.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("CA file and client certificate", func(t *testing.T) {
		caPath := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(caPath, []byte(caPEM), 0o600); err != nil {
			t.Fatal(err)
		}
		certPath, keyPath := writeClientCert(t)
		c := newTestClient(t, ts.URL, func(cfg *Config) {
			cfg.TLS = TLSConfig{CACertPath: caPath, ClientCertPath: certPath, ClientKeyPath: keyPath}
		})
		if _, err := c.Ping(t.Context()); err != nil || !gotClientCert {
			t.Fatalf("err=%v clientCert=%v", err, gotClientCert)
		}
	})
	t.Run("insecure skip verify", func(t *testing.T) {
		c := newTestClient(t, ts.URL, func(cfg *Config) { cfg.TLS.InsecureSkipVerify = true })
		if _, err := c.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("invalid TLS settings are CONFIG errors", func(t *testing.T) {
		bad := []TLSConfig{
			{CACertPEM: "not pem"},
			{CACertPath: filepath.Join(t.TempDir(), "missing.pem")},
			{ClientCertPath: "/nonexistent/c.pem", ClientKeyPath: "/nonexistent/k.pem"},
		}
		for i, tc := range bad {
			_, err := NewClient(Config{CommonConfig: CommonConfig{URL: ts.URL, TLS: tc}})
			if CodeOf(err) != ErrorCodeConfig {
				t.Errorf("case %d: %v", i, err)
			}
		}
	})
}
