package workflow

import (
	"time"

	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// propagateTimeSkippingForExecutionChain propagates both time skipping config and state to the next run in the chain.
func propagateTimeSkippingForExecutionChain(source *persistencespb.WorkflowExecutionInfo) (
	tsc *workflowpb.TimeSkippingConfig, initialSkip *durationpb.Duration) {
	tsc = source.GetTimeSkippingInfo().GetConfig()
	initialSkip = durationpb.New(accumulatedSkippedDuration(source))
	return
}

// propagateTimeSkippingForChild makes sure the start time of the child workflow execution
// is shifted forward by the accumulated skipped duration.
// FastForward is an extra registered call which is never propagated to children,
// and the rest of other config is controled by the DisableChildPropagation flag.
func propagateTimeSkippingForChild(source *persistencespb.WorkflowExecutionInfo) (tsc *workflowpb.TimeSkippingConfig, initialSkip *durationpb.Duration) {
	initialSkip = durationpb.New(accumulatedSkippedDuration(source))

	disableChildPropagation := source.GetTimeSkippingInfo().GetConfig().GetDisableChildPropagation()
	enabled := source.GetTimeSkippingInfo().GetConfig().GetEnabled()
	if !enabled || disableChildPropagation {
		return nil, durationpb.New(accumulatedSkippedDuration(source))
	}
	return &workflowpb.TimeSkippingConfig{
		Enabled:                 enabled,
		DisableChildPropagation: disableChildPropagation,
	}, initialSkip
}

func accumulatedSkippedDuration(source *persistencespb.WorkflowExecutionInfo) time.Duration {
	return source.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
}
