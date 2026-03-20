package timeskipper

import (
	"time"

	"go.temporal.io/server/common/clock"
)

type (
	TimeSkipper struct {
		// no mutex, will use workflow mutex
		Enabled            bool
		SkippedDurations   []time.Duration
		AdjustedTimeSource clock.TimeSource
	}
)

// PumpOnce is the main entry point for the time skipper
// when time skipper should be triggered, this method will be called
func (ts *TimeSkipper) PumpOnce() {
	if !ts.Enabled {
		return
	}
	// to be implemented
}
