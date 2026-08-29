package encoding

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aupv9/slipstream/internal/cdc"
	eventpb "github.com/aupv9/slipstream/internal/encoding/eventpb"
)

type protoEncoder struct{}

func (protoEncoder) Encode(ev cdc.ChangeEvent) ([]byte, error) {
	msg := &eventpb.ChangeEvent{
		SourceId: ev.SourceID,
		Schema:   ev.Schema,
		Table:    ev.Table,
		Op:       string(ev.Op),
		Position: ev.Position,
	}
	if !ev.CommitTS.IsZero() {
		msg.CommitTs = timestamppb.New(ev.CommitTS)
	}

	var err error
	if msg.Before, err = toStruct(ev.Before); err != nil {
		return nil, fmt.Errorf("encoding: before image at %s: %w", ev.Position, err)
	}
	if msg.After, err = toStruct(ev.After); err != nil {
		return nil, fmt.Errorf("encoding: after image at %s: %w", ev.Position, err)
	}

	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encoding: marshal event at %s: %w", ev.Position, err)
	}
	return b, nil
}

func (protoEncoder) ContentType() string { return "application/x-protobuf" }
func (protoEncoder) Format() Format      { return FormatProtobuf }

// toStruct converts a row image into a protobuf Struct.
//
// Row values come from database drivers as arbitrary Go types — time.Time,
// []byte, big decimals — which structpb does not accept directly. Anything it
// refuses falls back to its string form, so an unusual column type degrades to
// a readable value rather than failing the whole event.
func toStruct(values map[string]any) (*structpb.Struct, error) {
	if values == nil {
		return nil, nil
	}
	fields := make(map[string]*structpb.Value, len(values))
	for k, v := range values {
		val, err := structpb.NewValue(v)
		if err != nil {
			val = structpb.NewStringValue(fmt.Sprintf("%v", v))
		}
		fields[k] = val
	}
	return &structpb.Struct{Fields: fields}, nil
}
