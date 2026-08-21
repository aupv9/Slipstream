// Package stdout writes events as JSON lines. It exists for development and
// for verifying a pipeline end to end without a real destination.
package stdout

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aupv9/slipstream/internal/cdc"
)

// Sink prints each event as one JSON object per line.
type Sink struct {
	name string
	mu   sync.Mutex
	enc  *json.Encoder
}

// New builds a stdout sink.
func New(name string) *Sink {
	return NewWriter(name, os.Stdout)
}

// NewWriter builds a sink writing to w, which makes the sink testable.
func NewWriter(name string, w io.Writer) *Sink {
	return &Sink{name: name, enc: json.NewEncoder(w)}
}

// Name identifies the sink.
func (s *Sink) Name() string { return s.name }

// Write emits one line per event. Replays show up as duplicate lines, which is
// exactly what you want while debugging at-least-once behaviour.
func (s *Sink) Write(_ context.Context, batch []cdc.ChangeEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range batch {
		if err := s.enc.Encode(ev); err != nil {
			return fmt.Errorf("stdout sink: encode: %w", err)
		}
	}
	return nil
}

// Close is a no-op; os.Stdout is not ours to close.
func (s *Sink) Close() error { return nil }
