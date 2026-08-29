package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Health is what the readiness endpoint reports. One process can run several
// pipelines and be the leader for some and a standby for others, so state is
// kept per pipeline.
type Health struct {
	mu        sync.Mutex
	pipelines map[string]PipelineHealth
}

// PipelineHealth is one pipeline's state.
type PipelineHealth struct {
	Leader    bool
	Streaming bool
	LastError string
}

// SetRole records whether this instance leads a pipeline.
func (h *Health) SetRole(pipeline string, leader bool) {
	h.update(pipeline, func(p *PipelineHealth) { p.Leader = leader })
}

// SetStreaming records whether a pipeline's reader is attached.
func (h *Health) SetStreaming(pipeline string, streaming bool) {
	h.update(pipeline, func(p *PipelineHealth) { p.Streaming = streaming })
}

// SetError records the latest failure for a pipeline.
func (h *Health) SetError(pipeline string, err error) {
	h.update(pipeline, func(p *PipelineHealth) {
		if err == nil {
			p.LastError = ""
			return
		}
		p.LastError = err.Error()
	})
}

func (h *Health) update(pipeline string, mutate func(*PipelineHealth)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pipelines == nil {
		h.pipelines = make(map[string]PipelineHealth)
	}
	p := h.pipelines[pipeline]
	mutate(&p)
	h.pipelines[pipeline] = p
}

// Snapshot returns the current state of every pipeline.
func (h *Health) Snapshot() map[string]PipelineHealth {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]PipelineHealth, len(h.pipelines))
	for k, v := range h.pipelines {
		out[k] = v
	}
	return out
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
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))

	state := s.health.Snapshot()
	names := make([]string, 0, len(state))
	for name := range state {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		p := state[name]
		role := "standby"
		if p.Leader {
			role = "leader"
		}
		fmt.Fprintf(w, "pipeline: %s role: %s streaming: %v last_error: %s\n",
			name, role, p.Streaming, p.LastError)
	}
}
