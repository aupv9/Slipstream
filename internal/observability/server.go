package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Health is what the readiness endpoint reports.
type Health struct {
	// Leader is true on the instance currently holding the lease.
	Leader atomic.Bool
	// Streaming is true once the reader is attached and delivering.
	Streaming atomic.Bool
	// LastError holds the most recent pipeline failure, if any.
	lastErr atomic.Value
}

// SetError records the latest pipeline error for the status endpoint.
func (h *Health) SetError(err error) {
	if err == nil {
		h.lastErr.Store("")
		return
	}
	h.lastErr.Store(err.Error())
}

// LastError returns the most recent pipeline error.
func (h *Health) LastError() string {
	v, _ := h.lastErr.Load().(string)
	return v
}

// Server exposes /metrics, /healthz and /readyz.
//
// Liveness is deliberately generous: a standby is alive and well even though it
// is deliberately doing nothing. Readiness is the same, because taking a
// standby out of service would defeat the point of running one.
type Server struct {
	registry *Registry
	health   *Health
	log      *slog.Logger
	srv      *http.Server
	ln       net.Listener
}

// NewServer prepares the HTTP server. Nothing listens until Start.
func NewServer(addr string, registry *Registry, health *Health, log *slog.Logger) *Server {
	s := &Server{registry: registry, health: health, log: log.With("component", "observability")}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)

	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Start binds the listener and serves in the background. Binding synchronously
// means a port clash is reported at startup rather than swallowed.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("observability: listen on %s: %w", s.srv.Addr, err)
	}
	s.ln = ln
	s.log.Info("serving metrics and health", "addr", ln.Addr().String())

	go func() {
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("metrics server stopped", "err", err)
		}
	}()
	return nil
}

// Addr is the bound address, useful when the config asked for port 0.
func (s *Server) Addr() string {
	if s.ln == nil {
		return s.srv.Addr
	}
	return s.ln.Addr().String()
}

// Stop shuts the server down.
func (s *Server) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(s.registry.Render()))
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	role := "standby"
	if s.health.Leader.Load() {
		role = "leader"
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ready\nrole: %s\nstreaming: %v\nlast_error: %s\n",
		role, s.health.Streaming.Load(), s.health.LastError())
}
