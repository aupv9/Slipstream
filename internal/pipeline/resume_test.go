package pipeline

import (
	"testing"

	"github.com/aupv9/slipstream/internal/controlplane"
)

// The offset alone is not enough to decide a resume: an offset written while a
// snapshot was still running covers only part of the data.
func TestPlanResume(t *testing.T) {
	cases := []struct {
		name        string
		offset      string
		offsetFound bool
		state       controlplane.SnapshotState
		stateFound  bool
		wantForce   bool
		wantFrom    string
		wantResume  bool
	}{
		{
			name:        "completed snapshot with an offset resumes",
			offset:      "0/2000",
			offsetFound: true,
			state:       controlplane.SnapshotState{Phase: controlplane.SnapshotDone, Mode: controlplane.SnapshotSingle},
			stateFound:  true,
			wantForce:   false,
			wantFrom:    "0/2000",
		},
		{
			name:        "interrupted snapshot must not resume from its partial offset",
			offset:      "0/2000",
			offsetFound: true,
			state:       controlplane.SnapshotState{Phase: controlplane.SnapshotRunning, Mode: controlplane.SnapshotSingle},
			stateFound:  true,
			wantForce:   true,
			wantFrom:    "0/2000", // kept for the log line, not used as a start point
		},
		{
			name:       "fresh pipeline bootstraps",
			stateFound: false,
			wantForce:  true,
		},
		{
			name:        "interrupted chunked snapshot resumes instead of restarting",
			offset:      "0/2000",
			offsetFound: true,
			state: controlplane.SnapshotState{
				Phase:      controlplane.SnapshotRunning,
				Mode:       controlplane.SnapshotChunked,
				ChunkTable: "public.users",
				ChunkKey:   []byte(`[42]`),
			},
			stateFound: true,
			wantForce:  false,
			wantFrom:   "0/2000",
			wantResume: true,
		},
		{
			name:        "an offset with no recorded snapshot is not trusted",
			offset:      "0/2000",
			offsetFound: true,
			stateFound:  false,
			wantForce:   true,
			wantFrom:    "0/2000",
		},
		{
			name:       "completed snapshot without an offset bootstraps again",
			state:      controlplane.SnapshotState{Phase: controlplane.SnapshotDone, Mode: controlplane.SnapshotSingle},
			stateFound: true,
			wantForce:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planResume(tc.offset, tc.offsetFound, tc.state, tc.stateFound)
			if got.ForceBootstrap != tc.wantForce {
				t.Errorf("ForceBootstrap = %v, want %v (reason: %s)", got.ForceBootstrap, tc.wantForce, got.Reason)
			}
			if got.From != tc.wantFrom {
				t.Errorf("From = %q, want %q", got.From, tc.wantFrom)
			}
			if (got.ResumeSnapshot != nil) != tc.wantResume {
				t.Errorf("ResumeSnapshot = %+v, want present=%v", got.ResumeSnapshot, tc.wantResume)
			}
			if tc.wantResume && got.ResumeSnapshot != nil {
				if got.ResumeSnapshot.Table != tc.state.ChunkTable {
					t.Errorf("resume table = %q, want %q", got.ResumeSnapshot.Table, tc.state.ChunkTable)
				}
				if string(got.ResumeSnapshot.Key) != string(tc.state.ChunkKey) {
					t.Errorf("resume key = %q, want %q", got.ResumeSnapshot.Key, tc.state.ChunkKey)
				}
			}
			if got.Reason == "" {
				t.Error("every decision should explain itself in the log")
			}
		})
	}
}
