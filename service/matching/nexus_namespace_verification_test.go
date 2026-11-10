package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

func TestXNexusReply(t *testing.T) {
	t.Parallel()
	for _, queueKind := range []struct {
		name string
		kind enumspb.TaskQueueKind
	}{
		{"normal", enumspb.TASK_QUEUE_KIND_NORMAL},
		{"worker_commands", enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
	} {
		for _, method := range []string{"completed", "failed"} {
			t.Run(queueKind.name+"/"+method, func(t *testing.T) {
				t.Parallel()
				tv := testvars.New(t)
				victim := tv.WithNamespaceID("namespace-B").WithNamespaceName("namespace-B")
				attacker := tv.WithNamespaceID("namespace-A").WithNamespaceName("namespace-A")
				ctrl := gomock.NewController(t)
				nsRegistry := namespace.NewMockRegistry(ctrl)
				ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{
					Id: victim.NamespaceID().String(), Name: victim.NamespaceName().String(),
				}, nil, "active")
				nsRegistry.EXPECT().GetNamespaceByID(victim.NamespaceID()).Return(ns, nil)
				queue := &taskqueuepb.TaskQueue{Name: victim.TaskQueue().Name, Kind: queueKind.kind}
				partition, err := tqid.PartitionFromProto(queue, victim.NamespaceID().String(), enumspb.TASK_QUEUE_TYPE_NEXUS)
				require.NoError(t, err)
				pm := NewMocktaskQueuePartitionManager(ctrl)
				pm.EXPECT().WaitUntilInitialized(gomock.Any()).Return(nil)
				engine := &matchingEngineImpl{
					namespaceRegistry: nsRegistry,
					config:            &Config{MinDispatchTaskTimeout: func(string) time.Duration { return 0 }},
					partitions:        map[tqid.PartitionKey]taskQueuePartitionManager{partition.Key(): pm},
					nexusResults:      collection.NewSyncMap[string, chan *nexusResult](),
				}
				expected := &matchingservice.DispatchNexusTaskResponse{}
				// Capture the UUID allocated by the real dispatch path, then simulate a reply arriving on this host.
				pm.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, taskID string, req *matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error) {
						require.Equal(t, victim.NamespaceID().String(), req.GetNamespaceId())
						if method == "completed" {
							reply := &workflowservice.RespondNexusTaskCompletedRequest{Namespace: attacker.NamespaceName().String(), Response: &nexuspb.Response{}}
							_, err := engine.RespondNexusTaskCompleted(ctx, &matchingservice.RespondNexusTaskCompletedRequest{
								NamespaceId: attacker.NamespaceID().String(), TaskQueue: queue, TaskId: taskID, Request: reply,
							}, metrics.NoopMetricsHandler)
							require.NoError(t, err)
							expected.Outcome = &matchingservice.DispatchNexusTaskResponse_Response{Response: reply.Response}
						} else {
							reply := &workflowservice.RespondNexusTaskFailedRequest{Namespace: attacker.NamespaceName().String(), Failure: &failurepb.Failure{Message: "forged response"}}
							_, err := engine.RespondNexusTaskFailed(ctx, &matchingservice.RespondNexusTaskFailedRequest{
								NamespaceId: attacker.NamespaceID().String(), TaskQueue: queue, TaskId: taskID, Request: reply,
							}, metrics.NoopMetricsHandler)
							require.NoError(t, err)
							expected.Outcome = &matchingservice.DispatchNexusTaskResponse_Failure{Failure: reply.Failure}
						}
						return nil, nil
					},
				)
				actual, err := engine.DispatchNexusTask(t.Context(), &matchingservice.DispatchNexusTaskRequest{
					NamespaceId: victim.NamespaceID().String(), TaskQueue: queue,
				})
				require.NoError(t, err)
				protorequire.ProtoEqual(t, expected, actual)
				t.Log("Dispatch in namespace B consumed a response submitted as namespace A")
			})
		}
	}
}
