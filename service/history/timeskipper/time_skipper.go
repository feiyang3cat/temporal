package timeskipper

import (
	"errors"
	"sync"
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/service/history/tasks"
)

type (

	// TimeSkipperType defines the type of time skipping behavior
	TimeSkipperType int32

	// TimeSkipper holds information about time skipping for a workflow execution.
	// This allows workflows to skip forward in time while maintaining correct timer behavior.
	TimeSkipper struct {
		NamespaceID string
		SkipperID   string
		Mutex       sync.Mutex
		SkipperType TimeSkipperType // may be found unuseful?

		SkippedDurations   []time.Duration
		BaseTimeSource     clock.TimeSource
		AdjustedTimeSource clock.TimeSource

		// it this is an independent time skipper, this map will only have one entry
		ExecutionKeysToUnlockStatus map[chasm.ExecutionKey]bool
	}
)

func NewIndependentTimeSkipper(
	executionKey chasm.ExecutionKey,
	baseTimeSource clock.TimeSource,
	skippedDurations []time.Duration,
) *TimeSkipper {
	keys := []chasm.ExecutionKey{executionKey}
	return newTimeSkipper(TimeSkipperTypeIndependent, keys, baseTimeSource, skippedDurations)
}

func NewGroupTimeSkipper(
	executionKeys []chasm.ExecutionKey,
	baseTimeSource clock.TimeSource,
	skippedDurations []time.Duration,
) *TimeSkipper {
	return newTimeSkipper(TimeSkipperTypeGroup, executionKeys, baseTimeSource, skippedDurations)
}

func newTimeSkipper(
	skipperType TimeSkipperType,
	executionKeys []chasm.ExecutionKey,
	baseTimeSource clock.TimeSource,
	skippedDurations []time.Duration,
) *TimeSkipper {
	adjustedTimeSource := clock.NewTimeSkippingTimeSource(baseTimeSource)
	if skippedDurations == nil {
		skippedDurations = make([]time.Duration, 0)
	}
	adjustedTimeSource.AddSkippedTimes(skippedDurations)
	executionKeysToUnlockStatus := make(map[chasm.ExecutionKey]bool)
	for _, executionKey := range executionKeys {
		executionKeysToUnlockStatus[executionKey] = false
	}
	return &TimeSkipper{
		ExecutionKeysToUnlockStatus: executionKeysToUnlockStatus,
		BaseTimeSource:              baseTimeSource,
		AdjustedTimeSource:          adjustedTimeSource,
		SkippedDurations:            skippedDurations,
		SkipperType:                 skipperType,
	}
}

// PumpOnce is the main entry point for the time skipper
// when time skipper should be triggered, this method will be called
func (ts *TimeSkipper) PumpOnce() {
	// simplified process just for demo
	ts.lockAllExecutions()
	defer ts.unlockAllExecutions()

	if !ts.isAutoSkippable() {
		return
	}
	nextTimerTask, err := ts.findNextTimerTaskToSkip()
	if nextTimerTask == nil {
		return
	}
	if err != nil {
		return
	}
	ts.addTimeSkippedEvents(nextTimerTask)
	ts.updateMutableState(nextTimerTask)
}

// KEY-STEP-0: acquire per-execution lock
func (ts *TimeSkipper) lockAllExecutions() error {
	// acquire execution lock of all executions (will this make deadlock with other operations?)
	return errors.New("not implemented")
}

func (ts *TimeSkipper) unlockAllExecutions() error {
	return errors.New("not implemented")
}

// KEY-STEP-1: check if time skipping is auto-skippable
func (ts *TimeSkipper) isAutoSkippable() bool {
	if ts.isTimeSkipperUnlocked() {
		return true
	}
	// todo:
	// check IsAutoSkippable for each execution
	return false
}

// KEY-STEP-2: get the next timer task to skip
func (ts *TimeSkipper) findNextTimerTaskToSkip() (tasks.Task, error) {
	return nil, errors.New("not implemented")
}

// KEY-STEP-2: add events to all these executions
func (ts *TimeSkipper) addTimeSkippedEvents(nextTimerTask tasks.Task) error {
	return errors.New("not implemented")
}

// KEY-STEP-3: update mutable state of all these executions
func (ts *TimeSkipper) updateMutableState(nextTimerTask tasks.Task) error {
	// KEY-STEP-4: the most important step
	// in-memory change:
	// -update the time source of all these executions
	// -update the skipped durations of all these executions
	// -update the pending timer tasks of all these executions
	// persistence change:
	// -update the change into persistence
	// todo: key problems to solve
	return errors.New("not implemented")
}

func (ts *TimeSkipper) isTimeSkipperUnlocked() bool {
	for _, unlocked := range ts.ExecutionKeysToUnlockStatus {
		if !unlocked {
			return false
		}
	}
	return true
}

// SetExecutionUnlocked sets the unlocked state for a specific execution
func (ts *TimeSkipper) Unlock(key chasm.ExecutionKey) {
	ts.ExecutionKeysToUnlockStatus[key] = true
}

func (ts *TimeSkipper) Lock(key chasm.ExecutionKey) {
	ts.ExecutionKeysToUnlockStatus[key] = false
}

const (
	// TimeSkipperTypeIndependent means this workflow skips time independently
	TimeSkipperTypeIndependent TimeSkipperType = iota

	// TimeSkipperTypeGroup means this workflow is part of a group that skips time together
	TimeSkipperTypeGroup
)

// String returns a string representation of TimeSkipperType
func (t TimeSkipperType) String() string {
	switch t {
	case TimeSkipperTypeIndependent:
		return "Independent"
	case TimeSkipperTypeGroup:
		return "Group"
	default:
		return "Unknown"
	}
}
