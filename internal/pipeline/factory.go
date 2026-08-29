package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/sink"
	"github.com/aupv9/slipstream/internal/sink/kafkasink"
	"github.com/aupv9/slipstream/internal/sink/natssink"
	"github.com/aupv9/slipstream/internal/sink/pgupsert"
	"github.com/aupv9/slipstream/internal/sink/stdout"
	"github.com/aupv9/slipstream/internal/sink/webhook"
	"github.com/aupv9/slipstream/internal/source"
	"github.com/aupv9/slipstream/internal/source/mongo"
	"github.com/aupv9/slipstream/internal/source/mysql"
	sourcepg "github.com/aupv9/slipstream/internal/source/postgres"
)

// buildReader constructs the configured source reader.
func buildReader(cfg config.Source, log *slog.Logger) (source.Reader, error) {
	switch cfg.Type {
	case "postgres", "postgresql":
		return sourcepg.New(cfg.Postgres, cfg.ID, log), nil
	case "mysql":
		return mysql.New(cfg.MySQL, cfg.ID, log), nil
	case "mongodb", "mongo":
		return mongo.New(cfg.MongoDB, cfg.ID, log), nil
	default:
		return nil, fmt.Errorf("pipeline: unknown source type %q (want postgres, mysql or mongodb)", cfg.Type)
	}
}

// buildSinks constructs every configured sink. On error, sinks already built
// are closed so a bad config leaves no connections behind.
func buildSinks(ctx context.Context, cfgs []config.SinkConfig) ([]sink.Sink, error) {
	built := make([]sink.Sink, 0, len(cfgs))
	fail := func(err error) ([]sink.Sink, error) {
		for _, s := range built {
			_ = s.Close()
		}
		return nil, err
	}

	for _, c := range cfgs {
		switch c.Type {
		case "stdout":
			built = append(built, stdout.New(c.Name))
		case "webhook":
			s, err := webhook.New(c.Name, c.Webhook)
			if err != nil {
				return fail(err)
			}
			built = append(built, s)
		case "pgupsert":
			s, err := pgupsert.New(ctx, c.Name, c.PGUpsert)
			if err != nil {
				return fail(err)
			}
			built = append(built, s)
		case "nats":
			s, err := natssink.New(c.Name, c.NATS)
			if err != nil {
				return fail(err)
			}
			built = append(built, s)
		case "kafka":
			s, err := kafkasink.New(c.Name, c.Kafka)
			if err != nil {
				return fail(err)
			}
			built = append(built, s)
		default:
			return fail(fmt.Errorf("pipeline: unknown sink type %q for sink %q "+
				"(want stdout, webhook, pgupsert, nats or kafka)", c.Type, c.Name))
		}
	}
	return built, nil
}
