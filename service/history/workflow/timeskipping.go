package workflow

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/protobuf/types/known/durationpb"
)

// propagateTimeSkippingToNextRun propagates both time skipping config and state to the next run in
// the chain (CaN, retry, cron). The config is deep-cloned so the next run can mutate it without
// affecting the source.
func propagateTimeSkippingToNextRun(
	source *persistencespb.WorkflowExecutionInfo,
) (*commonpb.TimeSkippingConfig, *commonpb.TimeSkippingStatePropagation) {
	previousTSC := source.GetTimeSkippingInfo().GetConfig()

	// if disabled, we just return nil for the new TSC
	var newTSC *commonpb.TimeSkippingConfig
	if previousTSC.GetEnabled() {
		newTSC = common.CloneProto(previousTSC)
	}

	stateProp := &commonpb.TimeSkippingStatePropagation{
		InitialSkippedDuration: durationpb.New(accumulatedSkippedDuration(source)),
	}
	if ff := source.GetTimeSkippingInfo().GetFastForwardInfo(); ff != nil && !ff.GetHasReached() {
		stateProp.FastForwardTargetTime = ff.GetTargetTime()
	}
	return newTSC, stateProp
}

// propagateTimeSkippingToChild makes sure the start time of the child workflow execution
// is shifted forward by the accumulated skipped duration.
// FastForward is never propagated to children.
func propagateTimeSkippingToChild(
	source *persistencespb.WorkflowExecutionInfo,
) (*commonpb.TimeSkippingConfig, *commonpb.TimeSkippingStatePropagation) {
	accum := accumulatedSkippedDuration(source)
	var stateProp *commonpb.TimeSkippingStatePropagation
	if accum > 0 {
		stateProp = &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(accum),
		}
	}

	enabled := source.GetTimeSkippingInfo().GetConfig().GetEnabled()
	disableChildPropagation := source.GetTimeSkippingInfo().GetConfig().GetDisableChildPropagation()
	if !enabled || disableChildPropagation {
		return nil, stateProp
	}

	return &commonpb.TimeSkippingConfig{
		Enabled: enabled,
	}, stateProp
}

func accumulatedSkippedDuration(source *persistencespb.WorkflowExecutionInfo) time.Duration {
	return source.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
}

// applyTimeSkippingTransition mutates the execution's TimeSkippingInfo for a single skip transition:
// it advances AccumulatedSkippedDuration by (TargetTime - CurrentTime), toggles Config.Enabled, and
// marks the fast-forward target reached. It is the archetype-agnostic core shared by the workflow
// event path (ApplyWorkflowExecutionTimeSkippingTransitionedEvent) and the CHASM close-transaction
// path (RecordTimeSkippingTransition); the workflow path records the transition as a history event while
// CHASM (which has no history) applies it directly, but the TSI mutation is identical.
//
// A zero TargetTime means "no skip" (only valid together with DisabledAfterFastForward, e.g. a bound
// hit exactly). transition.CurrentTime must be in the virtual frame (ms.Now() / the event's virtual
// EventTime).
func (ms *MutableStateImpl) applyTimeSkippingTransition(transition chasm.TimeSkippingTransition) error {
	opTag := tag.WorkflowActionWorkflowExecutionTimeSkippingTransitioned
	invalidTransitionError := serviceerror.NewInternal("TimeSkippingTransitionedEvent failed to apply")
	tsi := ms.executionInfo.GetTimeSkippingInfo()
	if tsi == nil {
		ms.logError("TimeSkippingTransitionedEvent failed to apply: TimeSkippingInfo is nil", opTag)
		return invalidTransitionError
	}
	if transition.TargetTime.IsZero() && !transition.DisabledAfterFastForward {
		ms.logError("TimeSkippingTransitionedEvent failed to apply: TargetTime is nil and disabled after fast-forward is false", opTag)
		return invalidTransitionError
	}

	if tsi.GetAccumulatedSkippedDuration() == nil {
		tsi.AccumulatedSkippedDuration = durationpb.New(0)
	}
	accumulatedSkippedDuration := tsi.GetAccumulatedSkippedDuration().AsDuration()
	if !transition.TargetTime.IsZero() {
		accumulatedSkippedDuration += transition.TargetTime.Sub(transition.CurrentTime)
	}
	tsi.AccumulatedSkippedDuration = durationpb.New(accumulatedSkippedDuration)
	tsi.Config.Enabled = !transition.DisabledAfterFastForward

	if transition.DisabledAfterFastForward && tsi.GetFastForwardInfo() != nil {
		tsi.FastForwardInfo.HasReached = true
	}

	ms.timeSkippingInfoUpdated = true

	// Re-stamp the CHASM fast-forward wake so its wall-clock fire time tracks the new accumulated skip
	// (real fire = TargetTime - accumulatedSkippedDuration). Bundled with the accumulated update here
	// rather than routed through the chasm task pipeline; no-op for workflows and once the target is
	// reached.
	ms.regenerateChasmFastForwardWakeTask()
	return nil
}

// chasmNoEventID is the sentinel event ID used when (re)computing time-skipping info for CHASM
// executions, which have no history events to anchor the configuration to. Real event IDs start at
// 1, so -1 is unambiguous. For CHASM the fast-forward wake is emitted as a CHASM task during
// CloseTransaction (anchored on the VersionedTransition), so this stub only ends up in the unused
// FastForwardInfo.SourceEventId field.
//
// TODO(time-skipping/chasm): emit the fast-forward wake as a ChasmTaskPure on the root component and
// anchor its staleness check on the VersionedTransition (as ChasmTaskInfo does) rather than an
// event ID.
const chasmNoEventID int64 = -1

// SetTimeSkippingConfig sets the execution's time-skipping configuration for a CHASM execution. It
// backs chasm.MutableContext.SetTimeSkippingConfig (via the chasm.NodeBackend interface): the first
// call initializes the TimeSkippingInfo, and subsequent calls update the config in place (preserving
// the accumulated skipped duration). It reuses the workflow path's initTimeSkippingInfo /
// updateTimeSkippingInfo, which are archetype-aware (no physical workflow timer task for CHASM — see
// applyFastForward). CHASM executions have no history events to anchor to (see chasmNoEventID) and no
// propagated state on this path.
func (ms *MutableStateImpl) SetTimeSkippingConfig(config *commonpb.TimeSkippingConfig) {
	if ms.executionInfo.GetTimeSkippingInfo() == nil {
		ms.initTimeSkippingInfo(config, nil, chasmNoEventID)
	} else {
		ms.updateTimeSkippingInfo(config, chasmNoEventID)
	}
}

// RecordTimeSkippingTransition records a single time-skipping transition for the execution. It is the
// archetype-agnostic apply sink shared by both time-skipping front-ends: the CHASM framework's
// closeTransactionHandleTimeSkipping (which builds the transition for non-workflow CHASM executions)
// and the workflow event path (closeTransactionHandleTimeSkipping -> shouldExecuteTimeSkipping). The
// caller is responsible for computing the final transition — including the min of the component
// candidate and the fast-forward target; this method only records it.
//
// archetype selects how the skip is recorded:
//   - the workflow archetype records a history event (AddWorkflowExecutionTimeSkippingTransitionedEvent),
//     which in turn applies the transition to the TimeSkippingInfo;
//   - all other archetypes have no history, so the transition is applied directly to the TSI.
//
// For CHASM executions, after this returns the physical timer tasks regenerated later in the same
// CloseTransaction pick up the new accumulated offset via AddTasks -> ToRealTime, so no separate
// timer-task regeneration is needed.
func (ms *MutableStateImpl) RecordTimeSkippingTransition(
	ctx context.Context,
	transition chasm.TimeSkippingTransition,
	archetype chasm.ArchetypeID,
) error {
	if !transition.IsValid() {
		return nil
	}
	switch archetype {
	case chasm.WorkflowArchetypeID:
		// workflows require applying the transition through a history event
		_, err := ms.AddWorkflowExecutionTimeSkippingTransitionedEvent(
			ctx, transition.TargetTime, transition.DisabledAfterFastForward)
		return err
	default:
		return ms.applyTimeSkippingTransition(transition)
	}
}
