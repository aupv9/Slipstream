package observability

// Metric names. Kept in one place so the HELP text and the call sites cannot
// drift apart.
const (
	// MetricLeader is 1 on the instance holding the lease, 0 on standbys. Two
	// instances reporting 1 for the same pipeline means split brain.
	MetricLeader = "slipstream_leader"
	// MetricEventsRead counts events produced by the source reader.
	MetricEventsRead = "slipstream_events_read_total"
	// MetricEventsWritten counts events accepted by each sink.
	MetricEventsWritten = "slipstream_events_written_total"
	// MetricBatchesWritten counts successful sink writes.
	MetricBatchesWritten = "slipstream_batches_written_total"
	// MetricWriteFailures counts failed sink writes, before retries.
	MetricWriteFailures = "slipstream_write_failures_total"
	// MetricDeadLettered counts events parked instead of delivered. Any value
	// above zero is worth an alert.
	MetricDeadLettered = "slipstream_dead_lettered_total"
	// MetricQueueDepth is how many events are waiting for each sink.
	MetricQueueDepth = "slipstream_sink_queue_depth"
	// MetricLagBytes is how far the committed position trails the source's
	// current write position. This is the number to alert on.
	MetricLagBytes = "slipstream_source_lag_bytes"
	// MetricSnapshotRunning is 1 while an initial snapshot is in progress.
	MetricSnapshotRunning = "slipstream_snapshot_running"
	// MetricSnapshotRows counts rows emitted by initial snapshots.
	MetricSnapshotRows = "slipstream_snapshot_rows_total"
	// MetricCommits counts offset commits.
	MetricCommits = "slipstream_offset_commits_total"
	// MetricPipelineRestarts counts how often the pipeline loop restarted after
	// an error.
	MetricPipelineRestarts = "slipstream_pipeline_restarts_total"
)

// Register declares every metric with its help text.
func Register(r *Registry) {
	r.Define(MetricLeader, Gauge, "1 on the instance holding the pipeline lease, 0 otherwise.")
	r.Define(MetricEventsRead, Counter, "Change events produced by the source reader.")
	r.Define(MetricEventsWritten, Counter, "Change events accepted by a sink.")
	r.Define(MetricBatchesWritten, Counter, "Batches successfully written to a sink.")
	r.Define(MetricWriteFailures, Counter, "Sink write attempts that failed, counted before retries.")
	r.Define(MetricDeadLettered, Counter, "Events parked in dead_letters instead of delivered.")
	r.Define(MetricQueueDepth, Gauge, "Events queued for a sink.")
	r.Define(MetricLagBytes, Gauge, "Bytes between the source's current position and the committed offset.")
	r.Define(MetricSnapshotRunning, Gauge, "1 while an initial snapshot is running.")
	r.Define(MetricSnapshotRows, Counter, "Rows emitted by initial snapshots.")
	r.Define(MetricCommits, Counter, "Offset commits written to the control plane.")
	r.Define(MetricPipelineRestarts, Counter, "Times the pipeline restarted after an error.")
}
