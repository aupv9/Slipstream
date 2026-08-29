// Package config loads the single YAML file that describes one pipeline.
//
// One process runs exactly one pipeline; running the same file on two or more
// hosts is what gives the pipeline HA, since only the instance holding the
// lease attaches to the source.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from strings like "10s".
type Duration time.Duration

// UnmarshalYAML parses a Go duration string.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Config is the whole file.
type Config struct {
	// InstanceID identifies this process in the leases table. Defaults to
	// "<hostname>-<pid>", which is distinct enough to make split-brain
	// visible in the table if it ever happened.
	InstanceID   string       `yaml:"instance_id"`
	ControlPlane ControlPlane `yaml:"control_plane"`
	// Pipeline configures a single pipeline. Use Pipelines instead to run
	// several from one process; setting both is an error.
	Pipeline Pipeline `yaml:"pipeline"`
	// Pipelines runs more than one pipeline in the same process. Each keeps
	// its own lease, offset and sinks, so they fail over independently — one
	// process can be the leader for some and a standby for others.
	Pipelines     []Pipeline    `yaml:"pipelines"`
	Observability Observability `yaml:"observability"`
}

// Observability configures the metrics and health endpoints.
type Observability struct {
	// Addr is the listen address for /metrics, /healthz and /readyz. Empty
	// disables the endpoints entirely.
	Addr string `yaml:"addr"`
}

// ControlPlane points at the small Postgres holding offsets and leases.
type ControlPlane struct {
	DSN string `yaml:"dsn"`
	// LeaseTTL is how long a lease stays valid without renewal. Failover
	// takes at most this long.
	LeaseTTL Duration `yaml:"lease_ttl"`
	// LeaseRenew is the renewal period; keep it well under LeaseTTL so a
	// transient control-plane hiccup does not cost us the lease.
	LeaseRenew Duration `yaml:"lease_renew"`
	// AutoMigrate applies the embedded control-plane DDL on startup.
	AutoMigrate bool `yaml:"auto_migrate"`
}

// Pipeline is one source feeding a set of sinks.
type Pipeline struct {
	ID     string       `yaml:"id"`
	Source Source       `yaml:"source"`
	Sinks  []SinkConfig `yaml:"sinks"`
	// CommitInterval is how often the slowest sink cursor is promoted to the
	// pipeline offset (and acknowledged back to the source).
	CommitInterval Duration `yaml:"commit_interval"`
}

// Source selects and configures the reader.
type Source struct {
	// Type is one of postgres, mysql, mongodb.
	Type string `yaml:"type"`
	// ID is stamped on every event as source_id; it is part of the sink
	// dedupe key, so changing it re-delivers everything.
	ID       string   `yaml:"id"`
	Postgres Postgres `yaml:"postgres"`
	MySQL    MySQL    `yaml:"mysql"`
	MongoDB  MongoDB  `yaml:"mongodb"`
}

// Postgres configures the logical-replication reader.
type Postgres struct {
	// DSN must reach the primary with the replication privilege. The reader
	// appends replication=database itself for the streaming connection.
	DSN string `yaml:"dsn"`
	// Slot is the logical replication slot; it is created on first run and
	// then reused, which is what makes resume possible.
	Slot string `yaml:"slot"`
	// Publication lists the tables pgoutput will decode. Created from Tables
	// if missing.
	Publication string `yaml:"publication"`
	// Tables are "schema.table" names to capture.
	Tables []string `yaml:"tables"`
	// AutoAddTables adds tables that appear in Tables but are missing from an
	// existing publication. Those tables are captured streaming-only: they get
	// no snapshot, so rows written before they were added are never delivered.
	// Left false, a mismatch stops the pipeline with instructions instead.
	AutoAddTables bool `yaml:"auto_add_tables"`
	// Snapshot takes an initial consistent snapshot when no offset exists.
	Snapshot bool `yaml:"snapshot"`
	// SnapshotMode is "single" (default) or "chunked".
	//
	// single reads every table inside one exported-snapshot transaction: exact
	// and simple, but it holds a transaction open for as long as it takes and
	// cannot be resumed if it is interrupted.
	//
	// chunked reads primary-key ranges interleaved with the stream, using
	// watermarks to drop rows the stream has already superseded. It holds no
	// long transaction, survives a restart, and suits tables too large to read
	// in one pass. It needs PostgreSQL 14 or newer and a primary key on every
	// captured table.
	SnapshotMode string `yaml:"snapshot_mode"`
	// ChunkSize is how many rows one chunk reads. Defaults to 10000.
	ChunkSize int `yaml:"chunk_size"`
}

// MySQL configures the binlog reader (see internal/source/mysql).
type MySQL struct {
	// DSN is a go-sql-driver DSN, e.g. user:pass@tcp(host:3306)/db. The
	// account needs REPLICATION SLAVE and REPLICATION CLIENT.
	DSN string `yaml:"dsn"`
	// ServerID must be unique across the whole replication topology: MySQL
	// disconnects the older client when two register the same id.
	ServerID uint32 `yaml:"server_id"`
	// Tables are "database.table" names to capture. Empty captures every base
	// table in the DSN's database.
	Tables []string `yaml:"tables"`
	// Snapshot takes an initial consistent snapshot when there is nothing to
	// resume from.
	Snapshot bool `yaml:"snapshot"`
	// Heartbeat is how often the server should send a keepalive while idle.
	Heartbeat Duration `yaml:"heartbeat"`
}

// MongoDB configures the change-stream reader (see internal/source/mongo).
type MongoDB struct {
	// URI must reach a replica set or sharded cluster; a standalone mongod has
	// no change streams.
	URI      string `yaml:"uri"`
	Database string `yaml:"database"`
	// Collections to capture. Empty captures every collection in the database.
	Collections []string `yaml:"collections"`
	// Snapshot reads the collections before streaming when there is nothing to
	// resume from.
	Snapshot bool `yaml:"snapshot"`
	// FullDocument controls what an update carries; the default, updateLookup,
	// fetches the whole document so a sink can upsert it. "default" sends only
	// the changed fields.
	FullDocument string `yaml:"full_document"`
}

// SinkConfig configures one sink. Name must be unique inside the pipeline: it
// is the key of that sink's cursor row.
type SinkConfig struct {
	Name string `yaml:"name"`
	// Type is one of stdout, webhook, pgupsert.
	Type string `yaml:"type"`
	// QueueSize bounds how far this sink may lag before it applies
	// backpressure to the reader. This is the memory ceiling per sink.
	QueueSize int `yaml:"queue_size"`
	// BatchMaxEvents and BatchMaxWait control write batching.
	BatchMaxEvents int      `yaml:"batch_max_events"`
	BatchMaxWait   Duration `yaml:"batch_max_wait"`
	// RetryInitial and RetryMax bound the write backoff.
	RetryInitial Duration `yaml:"retry_initial"`
	RetryMax     Duration `yaml:"retry_max"`
	// Encoding is the wire format for sinks that ship raw event payloads
	// (nats, kafka, process): "json" (default) or "protobuf". Sinks with their
	// own representation — pgupsert writes SQL, webhook posts a JSON envelope,
	// stdout prints JSON lines — do not take one.
	Encoding string `yaml:"encoding"`
	// OnFailure decides what happens when a sink keeps rejecting a batch.
	//
	// "retry" (the default) retries forever: nothing is ever lost, but one
	// permanently rejected event stalls this sink until someone intervenes.
	// "dead_letter" gives up after MaxAttempts, parks the offending events in
	// the control plane's dead_letters table and moves on — the only place
	// Slipstream deliberately stops delivering an event.
	OnFailure string `yaml:"on_failure"`
	// MaxAttempts is the attempt limit for OnFailure = dead_letter.
	MaxAttempts int `yaml:"max_attempts"`

	Webhook  WebhookSink  `yaml:"webhook"`
	PGUpsert PGUpsertSink `yaml:"pgupsert"`
	NATS     NATSSink     `yaml:"nats"`
	Kafka    KafkaSink    `yaml:"kafka"`
	Process  ProcessSink  `yaml:"process"`
}

// ProcessSink hands events to a program over its standard input, so a sink can
// be written in any language without this project growing an RPC protocol.
type ProcessSink struct {
	// Command is the program and its arguments.
	Command []string `yaml:"command"`
	// Dir is the working directory for the child process.
	Dir string `yaml:"dir"`
	// Env entries are appended to the child's environment as KEY=VALUE.
	Env []string `yaml:"env"`
	// Timeout bounds how long one batch may take before the sink gives up and
	// the pipeline retries.
	Timeout Duration `yaml:"timeout"`
	// RestartOnError starts a fresh process after a failed batch, rather than
	// keeping one that may be in a bad state.
	RestartOnError bool `yaml:"restart_on_error"`
}

// NATSSink publishes events to NATS, one message per event.
type NATSSink struct {
	URL string `yaml:"url"`
	// SubjectPrefix is prepended to <schema>.<table>.
	SubjectPrefix string `yaml:"subject_prefix"`
	// CoreOnly publishes without JetStream. Core NATS does not acknowledge
	// publishes, so an event can be lost after Slipstream counted it
	// delivered; JetStream (the default) both acknowledges and deduplicates on
	// the message ID.
	CoreOnly bool `yaml:"core_only"`
	// MaxPending bounds in-flight async publishes.
	MaxPending int `yaml:"max_pending"`
	// AckWait bounds how long a batch waits for its acknowledgements.
	AckWait Duration `yaml:"ack_wait"`
	Token   string   `yaml:"token"`
	// CredentialsFile is a NATS .creds file for authentication.
	CredentialsFile string `yaml:"credentials_file"`
}

// KafkaSink publishes events to Kafka.
type KafkaSink struct {
	Brokers []string `yaml:"brokers"`
	// Topic pins every event to one topic. Left empty, the topic is
	// <topic_prefix>.<schema>.<table>.
	Topic       string `yaml:"topic"`
	TopicPrefix string `yaml:"topic_prefix"`
	// Keys maps "schema.table" to its key columns. With them, the partition
	// key is the row identity so changes to one row stay ordered; without
	// them the whole table shares one partition.
	Keys             map[string][]string `yaml:"keys"`
	Compression      string              `yaml:"compression"`
	AutoCreateTopics bool                `yaml:"auto_create_topics"`
	Timeout          Duration            `yaml:"timeout"`
}

// Snapshot modes for sources that support more than one.
const (
	SnapshotSingle  = "single"
	SnapshotChunked = "chunked"
)

// Failure policies for a sink that keeps rejecting a batch.
const (
	OnFailureRetry      = "retry"
	OnFailureDeadLetter = "dead_letter"
)

// WebhookSink posts batches of events as JSON.
type WebhookSink struct {
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
	Timeout Duration          `yaml:"timeout"`
}

// PGUpsertSink mirrors rows into a Postgres/warehouse table by primary key.
type PGUpsertSink struct {
	DSN string `yaml:"dsn"`
	// Schema is where target tables live; defaults to the event's own schema.
	Schema string `yaml:"schema"`
	// Keys maps "schema.table" to its key columns, used for the ON CONFLICT
	// target. A table with no entry here is rejected rather than silently
	// duplicated.
	Keys map[string][]string `yaml:"keys"`
	// SoftDelete keeps deleted rows and stamps a column instead of removing
	// them.
	SoftDelete bool   `yaml:"soft_delete"`
	DeletedCol string `yaml:"deleted_column"`
}

// Load reads, expands ${ENV} references, and validates the config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(os.ExpandEnv(string(raw))))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	if cfg.Pipeline.ID != "" && len(cfg.Pipelines) > 0 {
		return nil, fmt.Errorf("config: set either pipeline: or pipelines:, not both")
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// AllPipelines is every configured pipeline, however it was written.
func (c *Config) AllPipelines() []Pipeline {
	if len(c.Pipelines) > 0 {
		return c.Pipelines
	}
	if c.Pipeline.ID != "" {
		return []Pipeline{c.Pipeline}
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.InstanceID == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		c.InstanceID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if c.ControlPlane.LeaseTTL == 0 {
		c.ControlPlane.LeaseTTL = Duration(10 * time.Second)
	}
	if c.ControlPlane.LeaseRenew == 0 {
		c.ControlPlane.LeaseRenew = Duration(c.ControlPlane.LeaseTTL.D() / 3)
	}
	if len(c.Pipelines) == 0 && c.Pipeline.ID != "" {
		c.Pipelines = []Pipeline{c.Pipeline}
	}
	for i := range c.Pipelines {
		applyPipelineDefaults(&c.Pipelines[i])
	}
	if len(c.Pipelines) == 1 {
		c.Pipeline = c.Pipelines[0]
	}
}

func applyPipelineDefaults(p *Pipeline) {
	if p.CommitInterval == 0 {
		p.CommitInterval = Duration(time.Second)
	}
	if p.Source.ID == "" {
		p.Source.ID = p.ID
	}
	if p.Source.Postgres.SnapshotMode == "" {
		p.Source.Postgres.SnapshotMode = SnapshotSingle
	}
	if p.Source.Postgres.ChunkSize == 0 {
		p.Source.Postgres.ChunkSize = 10000
	}
	if p.Source.Postgres.Slot == "" {
		p.Source.Postgres.Slot = slug("slipstream_" + p.ID)
	}
	if p.Source.Postgres.Publication == "" {
		p.Source.Postgres.Publication = slug("slipstream_" + p.ID)
	}

	for i := range p.Sinks {
		s := &p.Sinks[i]
		if s.Name == "" {
			s.Name = s.Type
		}
		if s.QueueSize == 0 {
			s.QueueSize = 4096
		}
		if s.BatchMaxEvents == 0 {
			s.BatchMaxEvents = 500
		}
		if s.BatchMaxWait == 0 {
			s.BatchMaxWait = Duration(200 * time.Millisecond)
		}
		if s.RetryInitial == 0 {
			s.RetryInitial = Duration(250 * time.Millisecond)
		}
		if s.RetryMax == 0 {
			s.RetryMax = Duration(30 * time.Second)
		}
		if s.Webhook.Timeout == 0 {
			s.Webhook.Timeout = Duration(30 * time.Second)
		}
		if s.NATS.AckWait == 0 {
			s.NATS.AckWait = Duration(30 * time.Second)
		}
		if s.NATS.MaxPending == 0 {
			s.NATS.MaxPending = 256
		}
		if s.Kafka.Timeout == 0 {
			s.Kafka.Timeout = Duration(30 * time.Second)
		}
		if s.Process.Timeout == 0 {
			s.Process.Timeout = Duration(60 * time.Second)
		}
		if s.PGUpsert.DeletedCol == "" {
			s.PGUpsert.DeletedCol = "_deleted_at"
		}
		if s.OnFailure == "" {
			s.OnFailure = OnFailureRetry
		}
		if s.Encoding == "" {
			s.Encoding = "json"
		}
	}
}

func (c *Config) validate() error {
	if c.ControlPlane.DSN == "" {
		return fmt.Errorf("config: control_plane.dsn is required")
	}
	if c.ControlPlane.LeaseRenew.D() >= c.ControlPlane.LeaseTTL.D() {
		return fmt.Errorf("config: control_plane.lease_renew (%s) must be shorter than lease_ttl (%s)",
			c.ControlPlane.LeaseRenew.D(), c.ControlPlane.LeaseTTL.D())
	}
	if len(c.Pipelines) == 0 {
		return fmt.Errorf("config: define a pipeline (pipeline:) or several (pipelines:)")
	}

	seenPipelines := make(map[string]bool, len(c.Pipelines))
	for _, p := range c.Pipelines {
		if p.ID == "" {
			return fmt.Errorf("config: every pipeline needs an id")
		}
		if seenPipelines[p.ID] {
			return fmt.Errorf("config: duplicate pipeline id %q; ids key the leases and offsets", p.ID)
		}
		seenPipelines[p.ID] = true
		if err := validatePipeline(p); err != nil {
			return err
		}
	}
	return nil
}

func validatePipeline(p Pipeline) error {
	if p.Source.Type == "" {
		return fmt.Errorf("config: pipeline %q has no source.type", p.ID)
	}
	switch p.Source.Postgres.SnapshotMode {
	case SnapshotSingle, SnapshotChunked:
	default:
		return fmt.Errorf("config: pipeline %q has snapshot_mode %q, want %q or %q",
			p.ID, p.Source.Postgres.SnapshotMode, SnapshotSingle, SnapshotChunked)
	}
	if len(p.Sinks) == 0 {
		return fmt.Errorf("config: pipeline %q must list at least one sink", p.ID)
	}

	seen := make(map[string]bool, len(p.Sinks))
	for _, s := range p.Sinks {
		if s.Type == "" {
			return fmt.Errorf("config: pipeline %q has a sink %q with no type", p.ID, s.Name)
		}
		if seen[s.Name] {
			return fmt.Errorf("config: pipeline %q has duplicate sink name %q; names key the sink_cursor rows",
				p.ID, s.Name)
		}
		seen[s.Name] = true

		switch s.Type {
		case "nats", "kafka", "process":
		default:
			if s.Encoding != "json" {
				return fmt.Errorf("config: sink %q is of type %q, which has its own representation "+
					"and takes no encoding (got %q)", s.Name, s.Type, s.Encoding)
			}
		}

		switch s.OnFailure {
		case OnFailureRetry:
			if s.MaxAttempts > 0 {
				return fmt.Errorf("config: sink %q sets max_attempts with on_failure: retry, "+
					"which would drop events once the limit is hit; use on_failure: dead_letter "+
					"to park them instead, or remove max_attempts to retry forever", s.Name)
			}
		case OnFailureDeadLetter:
			if s.MaxAttempts <= 0 {
				return fmt.Errorf("config: sink %q uses on_failure: dead_letter but no max_attempts, "+
					"so nothing would ever be parked", s.Name)
			}
		default:
			return fmt.Errorf("config: sink %q has on_failure %q, want %q or %q",
				s.Name, s.OnFailure, OnFailureRetry, OnFailureDeadLetter)
		}
	}
	return nil
}

// slug makes an identifier safe to use unquoted as a slot/publication name.
func slug(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}
