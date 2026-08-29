// Package encoding turns change events into bytes for sinks that ship them
// somewhere else.
//
// JSON is the default because it is readable in a log, a queue browser or a
// webhook receiver, and debugging a pipeline is most of the work. Protobuf is
// there for when the volume makes that readability too expensive.
package encoding

import (
	"encoding/json"
	"fmt"

	"github.com/aupv9/slipstream/internal/cdc"
)

// Format names a wire format.
type Format string

const (
	// FormatJSON is the default: self-describing and debuggable.
	FormatJSON Format = "json"
	// FormatProtobuf is a compact binary encoding of the same event.
	FormatProtobuf Format = "protobuf"
)

// Encoder turns one event into bytes.
type Encoder interface {
	// Encode serialises the event.
	Encode(ev cdc.ChangeEvent) ([]byte, error)
	// ContentType describes the output, for sinks that carry one.
	ContentType() string
	// Format names the encoding.
	Format() Format
}

// New builds the encoder for a format. An empty format means JSON.
func New(format Format) (Encoder, error) {
	switch format {
	case "", FormatJSON:
		return jsonEncoder{}, nil
	case FormatProtobuf:
		return protoEncoder{}, nil
	default:
		return nil, fmt.Errorf("encoding: unknown format %q, want %q or %q",
			format, FormatJSON, FormatProtobuf)
	}
}

type jsonEncoder struct{}

func (jsonEncoder) Encode(ev cdc.ChangeEvent) ([]byte, error) {
	b, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("encoding: marshal event at %s: %w", ev.Position, err)
	}
	return b, nil
}

func (jsonEncoder) ContentType() string { return "application/json" }
func (jsonEncoder) Format() Format      { return FormatJSON }
