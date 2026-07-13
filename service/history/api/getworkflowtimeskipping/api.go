package getworkflowtimeskipping

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Invoke(
	ctx context.Context,
	req *historyservice.GetWorkflowTimeSkippingRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.GetWorkflowTimeSkippingResponse, retError error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return nil, err
	}

	execution := req.GetRequest().GetWorkflowExecution()
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			req.GetNamespaceId(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	// The lock is released before we return, after which mutable state may be mutated. Clone
	// everything referenced by the response since marshalling happens after we return.
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	mutableState := workflowLease.GetMutableState()

	response := &workflowservice.GetWorkflowTimeSkippingResponse{
		CurrentTime: timestamppb.New(mutableState.Now()),
	}
	if ffInfo := mutableState.GetExecutionInfo().GetTimeSkippingInfo().GetFastForwardInfo(); ffInfo != nil {
		response.FastForward = &commonpb.TimeSkippingFastForward{
			TargetTime:   ffInfo.GetTargetTime(),
			HasCompleted: ffInfo.GetHasReached(),
		}
	}

	return &historyservice.GetWorkflowTimeSkippingResponse{Response: response}, nil
}
