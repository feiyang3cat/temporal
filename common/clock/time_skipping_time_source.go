package clock

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// TimeSkippingTimeSource is a TimeSource that offsets Now() by the accumulated skipped duration
// stored in a TimeSkippingInfo. Call SetTimeSkippingInfo to wire it up; falls back to the base
// TimeSource when not set or when TimeSkippingInfo is nil. Methods of Since, AfterFunc, and NewTimer
// are not supported for time-skipping related features and will fallback to the base TimeSource.
type TimeSkippingTimeSource struct {
	base             TimeSource
	timeSkippingInfo *persistencespb.TimeSkippingInfo
}

func NewTimeSkippingTimeSource(base TimeSource) *TimeSkippingTimeSource {
	return &TimeSkippingTimeSource{base: base}
}

// SetTimeSkippingInfo sets the TimeSkippingInfo to read accumulated skipped duration from.
// Pass nil to fall back to the base TimeSource.
func (ts *TimeSkippingTimeSource) SetTimeSkippingInfo(info *persistencespb.TimeSkippingInfo) {
	ts.timeSkippingInfo = info
}

func (ts *TimeSkippingTimeSource) Now() time.Time {
	t := ts.base.Now()
	if ts.timeSkippingInfo != nil && ts.timeSkippingInfo.AccumulatedSkippedDuration != nil {
		t = t.Add(ts.timeSkippingInfo.AccumulatedSkippedDuration.AsDuration())
	}
	return t
}

// Since leverages the base TimeSource's Since method, and
// time-skipping related features are not supported.
// TODO@time-skipping: examine if there is any need to skip time for this method
func (ts *TimeSkippingTimeSource) Since(t time.Time) time.Duration {
	return ts.base.Since(t)
}

// AfterFunc leverages the base TimeSource's AfterFunc method, and
// time-skipping related features are not supported.
// TODO@time-skipping: examine if there is any need to skip time for this method
func (ts *TimeSkippingTimeSource) AfterFunc(d time.Duration, f func()) Timer {
	return ts.base.AfterFunc(d, f)
}

// NewTimer leverages the base TimeSource's NewTimer method, and
// time-skipping related features are not supported.
// TODO@time-skipping: examine if there is any need to skip time for this method
func (ts *TimeSkippingTimeSource) NewTimer(d time.Duration) (<-chan time.Time, Timer) {
	return ts.base.NewTimer(d)
}
