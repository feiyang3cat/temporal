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
	TimeSkipperKey struct {
		// option-1:
		// one way is to use the first executionKey the time skipper manages as the main executionKey
		// so that we can find the shardID from the main executionKey
		// the runID is not used to calculate the shardID, so it can be any value
		MainExecutionKey chasm.ExecutionKey

		// option-2:
		// this skipperID is a unique identifier for the time skipper, but right now
		// the shardID it belongs to is not calculated from this skipperID,
		// but from the executionKeys it manages
		NamespaceID string
		ShardID     int32  // this shardID is used to find the shard context
		SkipperID   string // we can use the first executionKey's runID as the skipperID
	}

	TimeSkipperPerExecutionInfos struct {
		Key              TimeSkipperKey
		UnlockStatus     bool
		SkippedDurations []time.Duration // the timeskipping timesource can be uniquely built from the skipped durations
	}

	// TimeSkipper is managed by the shard, can be in cache/memory,
	// and should be stored in the persistence layer.
	TimeSkipper struct {
		Key  TimeSkipperKey
		lock sync.Mutex

		SkippedDurations   []time.Duration
		BaseTimeSource     clock.TimeSource // should be the shard context's time source
		AdjustedTimeSource clock.TimeSource

		// it this is an independent time skipper, this map will only have one entry
		// compatible with the old workflowKey
		ExecutionKeysToUnlockStatus map[chasm.ExecutionKey]bool
	}
)

func NewTimeSkipper(
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
	}
}

// PumpOnce is the main entry point for the time skipper
// when time skipper should be triggered, this method will be called
func (ts *TimeSkipper) PumpOnce() {
	// simplified process just for demo
	if err := ts.lockAllExecutions(); err != nil {
		return
	}
	defer func() { _ = ts.unlockAllExecutions() }()

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
	if err := ts.addTimeSkippedEvents(nextTimerTask); err != nil {
		return
	}
	if err := ts.updateMutableState(nextTimerTask); err != nil {
		return
	}
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

// Unlock sets the unlocked state for a specific execution.
func (ts *TimeSkipper) Unlock(key chasm.ExecutionKey) {
	ts.ExecutionKeysToUnlockStatus[key] = true
}

func (ts *TimeSkipper) Lock(key chasm.ExecutionKey) {
	ts.ExecutionKeysToUnlockStatus[key] = false
}
