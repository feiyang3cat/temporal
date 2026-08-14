package tdbg

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
)

type (
	batchTestAdminClient struct {
		adminservice.AdminServiceClient
		currentCluster string
		lastRequest    *adminservice.StartAdminBatchOperationRequest
	}

	batchTestWorkflowClient struct {
		workflowservice.WorkflowServiceClient
		isGlobalNamespace bool
		activeCluster     string
		missingNamespaces map[string]bool
		emptyIDNamespaces map[string]bool
	}

	batchTestClient struct {
		admin    *batchTestAdminClient
		workflow *batchTestWorkflowClient
	}

	batchCommandTestSuite struct {
		*require.Assertions
		suite.Suite
		app    *cli.App
		client *batchTestClient
		output bytes.Buffer
	}
)

func (t *batchTestClient) AdminClient(*cli.Context) adminservice.AdminServiceClient {
	return t.admin
}

func (t *batchTestClient) WorkflowClient(*cli.Context) workflowservice.WorkflowServiceClient {
	return t.workflow
}

func (t *batchTestWorkflowClient) DescribeNamespace(
	_ context.Context,
	request *workflowservice.DescribeNamespaceRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeNamespaceResponse, error) {
	if t.missingNamespaces[request.GetNamespace()] {
		return nil, serviceerror.NewNamespaceNotFound(request.GetNamespace())
	}
	namespaceID := request.GetNamespace() + "-id"
	if t.emptyIDNamespaces[request.GetNamespace()] {
		namespaceID = ""
	}
	return &workflowservice.DescribeNamespaceResponse{
		NamespaceInfo:     &namespacepb.NamespaceInfo{Id: namespaceID},
		IsGlobalNamespace: t.isGlobalNamespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: t.activeCluster,
		},
	}, nil
}

func (t *batchTestAdminClient) DescribeCluster(
	context.Context,
	*adminservice.DescribeClusterRequest,
	...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {
	return &adminservice.DescribeClusterResponse{ClusterName: t.currentCluster}, nil
}

func (t *batchTestWorkflowClient) CountWorkflowExecutions(
	context.Context,
	*workflowservice.CountWorkflowExecutionsRequest,
	...grpc.CallOption,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	return &workflowservice.CountWorkflowExecutionsResponse{Count: 3}, nil
}

func (t *batchTestWorkflowClient) CountActivityExecutions(
	context.Context,
	*workflowservice.CountActivityExecutionsRequest,
	...grpc.CallOption,
) (*workflowservice.CountActivityExecutionsResponse, error) {
	return &workflowservice.CountActivityExecutionsResponse{Count: 5}, nil
}

func (t *batchTestAdminClient) StartAdminBatchOperation(
	_ context.Context,
	request *adminservice.StartAdminBatchOperationRequest,
	_ ...grpc.CallOption,
) (*adminservice.StartAdminBatchOperationResponse, error) {
	t.lastRequest = request
	return &adminservice.StartAdminBatchOperationResponse{}, nil
}

const testCurrentCluster = "active-cluster"

func TestBatchCommandSuite(t *testing.T) {
	suite.Run(t, new(batchCommandTestSuite))
}

func (s *batchCommandTestSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.client = &batchTestClient{
		admin: &batchTestAdminClient{currentCluster: testCurrentCluster},
		workflow: &batchTestWorkflowClient{
			activeCluster:     testCurrentCluster,
			missingNamespaces: make(map[string]bool),
			emptyIDNamespaces: make(map[string]bool),
		},
	}
	s.app = NewCliApp(func(params *Params) {
		params.ClientFactory = s.client
		params.Writer = &s.output
		params.ErrWriter = &s.output
	})
	s.app.ExitErrHandler = func(*cli.Context, error) {}
}

func (s *batchCommandTestSuite) run(args ...string) error {
	s.output.Reset()
	return s.app.Run(append([]string{"tdbg", "--namespace", "target-ns", "--yes", "delegated-batch", "start"}, args...))
}

func (s *batchCommandTestSuite) TestAdminBatchStart() {
	s.Run("Terminate populates the admin envelope", func() {
		s.NoError(s.run(
			"--batch-type", batchTypeTerminateWorkflows,
			"--query", "WorkflowType='MyWorkflow'",
			"--reason", "cleanup",
			"--job-id", "my-job",
		))

		request := s.client.admin.lastRequest
		s.NotNil(request)
		if request == nil {
			return
		}
		s.Equal("target-ns", request.GetNamespace())
		s.Equal("WorkflowType='MyWorkflow'", request.GetVisibilityQuery())
		s.Equal("cleanup", request.GetReason())
		s.Equal("my-job:target-ns", request.GetJobId())
		s.Equal(
			[]*adminservice.TargetNamespace{{Namespace: "target-ns", NamespaceId: "target-ns-id"}},
			request.GetTargetNamespaces(),
		)
		s.Equal(enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW, request.GetDelegationOperation().GetBatchType())
		s.Contains(s.output.String(), "DANGER: destructive delegated batch operation")
		s.Contains(s.output.String(), "User namespace: \"target-ns\"")
		s.Contains(s.output.String(), "Batch workflow namespace: \"temporal-system\"")
		s.Contains(s.output.String(), "Operation: terminate-workflows")
		s.Contains(s.output.String(), "Currently matching: 3 workflows")
	})

	s.Run("Terminate activities delegates the activity batch type", func() {
		s.NoError(s.run(
			"--batch-type", batchTypeTerminateActivities,
			"--query", "A=B",
			"--reason", "stuck activities",
		))

		request := s.client.admin.lastRequest
		s.Equal(enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY, request.GetDelegationOperation().GetBatchType())
		// The operation itself needs no payload: identity and reason travel on the envelope.
		s.Equal("stuck activities", request.GetReason())
		s.NotEmpty(request.GetIdentity())
		s.Contains(s.output.String(), "Operation: terminate-activities")
		s.Contains(s.output.String(), "Currently matching: 5 activities")
	})

	s.Run("Missing namespaces can be skipped", func() {
		s.client.workflow.missingNamespaces["missing"] = true
		defer delete(s.client.workflow.missingNamespaces, "missing")
		s.client.admin.lastRequest = nil

		s.NoError(s.run(
			"--namespaces", "target-ns",
			"--namespaces", "missing",
			"--batch-type", batchTypeTerminateWorkflows,
			"--query", "A=B",
			"--reason", "cleanup",
			"--job-id", "partial-job",
		))

		request := s.client.admin.lastRequest
		s.NotNil(request)
		s.Equal("partial-job:target-ns", request.GetJobId())
		s.Equal(
			[]*adminservice.TargetNamespace{{Namespace: "target-ns", NamespaceId: "target-ns-id"}},
			request.GetTargetNamespaces(),
		)
		s.Contains(s.output.String(), `namespaces "missing" could not be resolved`)
	})

	s.Run("Unknown batch type is rejected", func() {
		s.ErrorContains(s.run("--batch-type", "nonsense", "--query", "A=B", "--reason", "r"), "unknown batch type")
	})

	s.Run("Query is required", func() {
		s.ErrorContains(s.run("--batch-type", batchTypeTerminateWorkflows, "--reason", "r"), FlagVisibilityQuery)
	})

	s.Run("Reason is required", func() {
		s.ErrorContains(s.run("--batch-type", batchTypeTerminateWorkflows, "--query", "A=B"), FlagReason)
	})

	s.Run("Global namespace active in this cluster is allowed", func() {
		s.client.workflow.isGlobalNamespace = true
		s.client.workflow.activeCluster = testCurrentCluster
		s.NoError(s.run("--batch-type", batchTypeTerminateWorkflows, "--query", "A=B", "--reason", "r"))
	})

	s.Run("Global namespace active in another cluster is rejected", func() {
		s.client.workflow.isGlobalNamespace = true
		s.client.workflow.activeCluster = "other-cluster"
		s.client.admin.lastRequest = nil
		err := s.run("--batch-type", batchTypeTerminateWorkflows, "--query", "A=B", "--reason", "r")
		s.ErrorContains(err, "must be started in the active cluster")
		s.Nil(s.client.admin.lastRequest, "the job must not be started")
	})
}

type batchAutoConfirmFlagLookup bool

func (a batchAutoConfirmFlagLookup) Bool(string) bool {
	return bool(a)
}

func TestResolveTargetNamespaces(t *testing.T) {
	workflowClient := &batchTestWorkflowClient{
		missingNamespaces: map[string]bool{"missing": true},
		emptyIDNamespaces: map[string]bool{"empty-id": true},
	}
	var output bytes.Buffer
	prompter := NewPrompter(batchAutoConfirmFlagLookup(false), func(params *PrompterParams) {
		params.Reader = strings.NewReader("yes\n")
		params.Writer = &output
		params.Exiter = func(int) { t.Fatal("confirmation unexpectedly rejected") }
	})

	targetNamespaces, err := resolveTargetNamespaces(
		context.Background(),
		workflowClient,
		[]string{"valid", "missing", "empty-id"},
		&output,
		prompter,
	)
	require.NoError(t, err)
	require.Equal(t, []*adminservice.TargetNamespace{{Namespace: "valid", NamespaceId: "valid-id"}}, targetNamespaces)
	require.Contains(t, output.String(), `namespaces "missing,empty-id" could not be resolved`)
	require.Contains(t, output.String(), `Choose no to correct the namespace names and retry.`)

	_, err = resolveTargetNamespaces(
		context.Background(),
		workflowClient,
		[]string{"missing", "empty-id"},
		&output,
		prompter,
	)
	require.ErrorContains(t, err, "none of the requested namespaces could be resolved")
}
