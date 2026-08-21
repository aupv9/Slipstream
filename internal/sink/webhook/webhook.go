// Package webhook POSTs batches of change events as JSON.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
)

// Sink delivers batches to an HTTP endpoint.
type Sink struct {
	name   string
	cfg    config.WebhookSink
	client *http.Client
}

// New builds a webhook sink.
func New(name string, cfg config.WebhookSink) (*Sink, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("webhook sink %q: url is required", name)
	}
	return &Sink{
		name:   name,
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.Timeout.D()},
	}, nil
}

// Name identifies the sink.
func (s *Sink) Name() string { return s.name }

// payload is the request body: a batch plus the positions it spans, so the
// receiver can recognize a replayed batch cheaply.
type payload struct {
	SourceID string            `json:"source_id"`
	Count    int               `json:"count"`
	First    string            `json:"first_position"`
	Last     string            `json:"last_position"`
	Events   []cdc.ChangeEvent `json:"events"`
}

// Write posts the batch. Any non-2xx response is an error, which the pipeline
// retries with backoff — so the endpoint must be idempotent. The
// Idempotency-Key header carries the batch's identity for that purpose;
// per-event identity is cdc.ChangeEvent.IdempotencyKey().
func (s *Sink) Write(ctx context.Context, batch []cdc.ChangeEvent) error {
	if len(batch) == 0 {
		return nil
	}

	body := payload{
		SourceID: batch[0].SourceID,
		Count:    len(batch),
		First:    batch[0].Position,
		Last:     batch[len(batch)-1].Position,
		Events:   batch,
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("webhook sink %q: marshal: %w", s.name, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.URL, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("webhook sink %q: build request: %w", s.name, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", fmt.Sprintf("%s|%s|%s", body.SourceID, body.First, body.Last))
	for k, v := range s.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook sink %q: post: %w", s.name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("webhook sink %q: status %d: %s", s.name, resp.StatusCode, bytes.TrimSpace(snippet))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	return nil
}

// Close releases idle connections.
func (s *Sink) Close() error {
	s.client.CloseIdleConnections()
	return nil
}
