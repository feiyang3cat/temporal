package workflow

import (
	"time"

	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
)

// propagateTimeSkippingToNextRun propagates both time skipping config and state to the next run in
// the chain (CaN, retry, cron). The config is deep-cloned so the next run can mutate it without
// affecting the source. FastForwardTargetTime is the source of truth for chain handoffs: the
// absolute virtual-time target carries forward unchanged so each new run does not re-anchor the
// target from the duration.
func propagateTimeSkippingToNextRun(
	source *persistencespb.WorkflowExecutionInfo,
) (*workflowpb.TimeSkippingConfig, *workflowpb.TimeSkippingStatePropagation) {
	var tsc *workflowpb.TimeSkippingConfig
	if cfg := source.GetTimeSkippingInfo().GetConfig(); cfg != nil {
		tsc = common.CloneProto(cfg)
	}
	stateProp := &workflowpb.TimeSkippingStatePropagation{
		InitialSkippedDuration: durationpb.New(accumulatedSkippedDuration(source)),
	}
	// Carry the active fast-forward target time forward so applyFastForward on the new run
	// can use it directly instead of recomputing target = now + (ff - accumulated), which
	// is wrong after any update or multi-run chain.
	if ff := source.GetTimeSkippingInfo().GetFastForward(); ff != nil && !ff.GetHasReached() {
		stateProp.FastForwardTargetTime = ff.GetTargetTime()
	}
	return tsc, stateProp
}

// propagateTimeSkippingToChild makes sure the start time of the child workflow execution
// is shifted forward by the accumulated skipped duration.
// FastForward is an extra registered call which is never propagated to children,
// and the rest of other config is controlled by the DisableChildPropagation flag.
func propagateTimeSkippingToChild(
	source *persistencespb.WorkflowExecutionInfo,
) (*workflowpb.TimeSkippingConfig, *workflowpb.TimeSkippingStatePropagation) {
	disableChildPropagation := source.GetTimeSkippingInfo().GetConfig().GetDisableChildPropagation()
	enabled := source.GetTimeSkippingInfo().GetConfig().GetEnabled()
	if !enabled || disableChildPropagation {
		return nil, nil
	}
	return &workflowpb.TimeSkippingConfig{
		Enabled:                 enabled,
		DisableChildPropagation: disableChildPropagation,
	}, &workflowpb.TimeSkippingStatePropagation{
		InitialSkippedDuration: durationpb.New(accumulatedSkippedDuration(source)),
		// FastForwardTargetTime intentionally not set: FastForward never cascades to children.
	}
}

func accumulatedSkippedDuration(source *persistencespb.WorkflowExecutionInfo) time.Duration {
	return source.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
}
