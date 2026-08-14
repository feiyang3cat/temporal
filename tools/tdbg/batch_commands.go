package tdbg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/primitives"
)

const (
	batchTypeTerminateWorkflows  = "terminate-workflows"
	batchTypeTerminateActivities = "terminate-activities"
)

var batchTypes = []string{
	batchTypeTerminateWorkflows,
	batchTypeTerminateActivities,
}

func newAdminBatchCommands(clientFactory ClientFactory, prompterFactory PrompterFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "start",
			Usage: "Delegate termination in a user namespace to a batch workflow in temporal-system",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagBatchType,
					Usage:    fmt.Sprintf("Batch operation to run, one of: %s", strings.Join(batchTypes, ", ")),
					Required: true,
				},
				&cli.StringFlag{
					Name:  FlagVisibilityQuery,
					Usage: "Visibility query selecting the executions to operate on",
				},
				&cli.StringSliceFlag{
					Name:  FlagNamespaces,
					Usage: "Namespaces targeted by the batch job",
				},
				&cli.StringFlag{
					Name:  FlagReason,
					Usage: "Reason for starting the batch job",
				},
				&cli.StringFlag{
					Name:  FlagJobID,
					Usage: "Optional job ID (auto-generated if not provided)",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminBatchStart(c, clientFactory, prompterFactory(c))
			},
		},
	}
}

// AdminBatchStart starts a batch operation whose workflow runs in the system namespace and
// whose per-execution calls target the namespace given by --namespace. Unlike
// StartBatchOperation, this does not require the target namespace to have a per-namespace
// worker in this cluster.
func AdminBatchStart(c *cli.Context, clientFactory ClientFactory, prompter *Prompter) error {
	adminClient := clientFactory.AdminClient(c)
	workflowClient := clientFactory.WorkflowClient(c)

	nsNames, err := getBatchNamespaces(c)
	if err != nil {
		return err
	}
	query, err := getRequiredOption(c, FlagVisibilityQuery)
	if err != nil {
		return err
	}

	reason, err := getRequiredOption(c, FlagReason)
	if err != nil {
		return err
	}

	batchType, err := getRequiredOption(c, FlagBatchType)
	if err != nil {
		return err
	}

	delegatedType, err := delegatedBatchType(batchType)
	if err != nil {
		return err
	}

	jobID := c.String(FlagJobID)
	if jobID == "" {
		jobID = fmt.Sprintf("batch-%s-%d", batchType, time.Now().UnixNano())
	}
	ctx, cancel := newContext(c)
	defer cancel()

	targetNamespaces, err := resolveTargetNamespaces(ctx, workflowClient, nsNames, c.App.Writer, prompter)
	if err != nil {
		return err
	}
	nsNames = targetNamespaceNames(targetNamespaces)
	nsName := nsNames[0]
	// The workflow ID lives in the system namespace, so it has to distinguish target namespaces.
	jobIDWithNS := fmt.Sprintf("%s:%s", jobID, strings.Join(nsNames, ","))

	var matchCount int64
	var targetKind string
	for _, targetNamespace := range nsNames {
		if err := checkTargetNamespaceActive(ctx, adminClient, workflowClient, targetNamespace); err != nil {
			return err
		}
		count, kind, err := countDelegatedBatchExecutions(ctx, workflowClient, targetNamespace, query, delegatedType)
		if err != nil {
			return err
		}
		matchCount += count
		targetKind = kind
	}
	namespaceLabel := "namespace"
	if len(nsNames) > 1 {
		namespaceLabel = "namespaces"
	}

	summary := fmt.Sprintf(
		"DANGER: destructive delegated batch operation\n\n"+
			"User %s: %q\n"+
			"Batch workflow namespace: %q\n"+
			"Cluster eligibility: passed\n"+
			"Operation: %s\n"+
			"Visibility query: %q\n"+
			"Currently matching: %d %s\n\n"+
			"This delegates termination of matching %s in user %s %q to a batch workflow running in %q.\n"+
			"For a global namespace, this operation must be submitted through its active cluster.\n"+
			"Supported operations are limited to %s and %s.\n\n"+
			"Review the user namespace, visibility query, and current match count carefully.\n"+
			"Visibility results can change while the batch is running.",
		namespaceLabel,
		strings.Join(nsNames, ","),
		primitives.SystemLocalNamespace,
		batchType,
		query,
		matchCount,
		targetKind,
		targetKind,
		namespaceLabel,
		strings.Join(nsNames, ","),
		primitives.SystemLocalNamespace,
		batchTypeTerminateWorkflows,
		batchTypeTerminateActivities,
	)
	if _, err := fmt.Fprintln(c.App.Writer, summary); err != nil {
		return fmt.Errorf("unable to write batch operation summary: %w", err)
	}
	prompter.Prompt(fmt.Sprintf("Proceed with terminating the currently matching %d %s?", matchCount, targetKind))

	_, err = adminClient.StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace:        nsName,
		TargetNamespaces: targetNamespaces,
		VisibilityQuery:  query,
		JobId:            jobIDWithNS,
		Reason:           reason,
		Identity:         getCurrentUserFromEnv(),
		Operation: &adminservice.StartAdminBatchOperationRequest_DelegationOperation{
			DelegationOperation: &adminservice.BatchOperationDelegation{BatchType: delegatedType},
		},
	})
	if err != nil {
		return fmt.Errorf("unable to start batch operation: %w", err)
	}

	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintf(c.App.Writer,
		"Batch operation %q started successfully in namespace %s, targeting %s %q, with Job ID: %s\n",
		batchType, primitives.SystemLocalNamespace, namespaceLabel, strings.Join(nsNames, ","), jobIDWithNS)
	return nil
}

func getBatchNamespaces(c *cli.Context) ([]string, error) {
	namespaces := c.StringSlice(FlagNamespaces)
	if len(namespaces) == 0 {
		namespaceName, err := getRequiredOption(c, FlagNamespace)
		if err != nil {
			return nil, err
		}
		namespaces = []string{namespaceName}
	}
	seen := make(map[string]struct{}, len(namespaces))
	for i, namespaceName := range namespaces {
		namespaceName = strings.TrimSpace(namespaceName)
		if namespaceName == "" {
			return nil, errors.New("namespace is empty")
		}
		if _, ok := seen[namespaceName]; ok {
			return nil, fmt.Errorf("namespace %q is duplicated", namespaceName)
		}
		seen[namespaceName] = struct{}{}
		namespaces[i] = namespaceName
	}
	return namespaces, nil
}

func resolveTargetNamespaces(
	ctx context.Context,
	workflowClient workflowservice.WorkflowServiceClient,
	names []string,
	writer io.Writer,
	prompter *Prompter,
) ([]*adminservice.TargetNamespace, error) {
	targetNamespaces := make([]*adminservice.TargetNamespace, 0, len(names))
	unresolvedNames := make([]string, 0)
	for _, name := range names {
		response, err := workflowClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: name})
		if err != nil {
			var namespaceNotFound *serviceerror.NamespaceNotFound
			if errors.As(err, &namespaceNotFound) {
				unresolvedNames = append(unresolvedNames, name)
				continue
			}
			return nil, fmt.Errorf("unable to describe namespace %q: %w", name, err)
		}
		namespaceID := response.GetNamespaceInfo().GetId()
		if namespaceID == "" {
			unresolvedNames = append(unresolvedNames, name)
			continue
		}
		targetNamespaces = append(targetNamespaces, &adminservice.TargetNamespace{
			Namespace:   name,
			NamespaceId: namespaceID,
		})
	}
	if len(unresolvedNames) == 0 {
		return targetNamespaces, nil
	}
	if len(targetNamespaces) == 0 {
		return nil, fmt.Errorf(
			"none of the requested namespaces could be resolved to namespace IDs: %q; correct the namespace names and retry",
			strings.Join(unresolvedNames, ","),
		)
	}
	if _, err := fmt.Fprintf(
		writer,
		"WARNING: namespaces %q could not be resolved to namespace IDs.\n",
		strings.Join(unresolvedNames, ","),
	); err != nil {
		return nil, fmt.Errorf("unable to write unresolved namespace warning: %w", err)
	}
	prompter.Prompt(fmt.Sprintf(
		"Continue with the remaining namespaces %q? Choose no to correct the namespace names and retry.",
		strings.Join(targetNamespaceNames(targetNamespaces), ","),
	))
	return targetNamespaces, nil
}

func targetNamespaceNames(targetNamespaces []*adminservice.TargetNamespace) []string {
	names := make([]string, 0, len(targetNamespaces))
	for _, targetNamespace := range targetNamespaces {
		names = append(names, targetNamespace.GetNamespace())
	}
	return names
}

func countDelegatedBatchExecutions(
	ctx context.Context,
	workflowClient workflowservice.WorkflowServiceClient,
	nsName string,
	query string,
	batchType enumspb.BatchOperationType,
) (int64, string, error) {
	switch batchType {
	case enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW:
		resp, err := workflowClient.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: nsName,
			Query:     query,
		})
		if err != nil {
			return 0, "", fmt.Errorf("unable to count workflow executions: %w", err)
		}
		return resp.GetCount(), "workflows", nil
	case enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY:
		resp, err := workflowClient.CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
			Namespace: nsName,
			Query:     query,
		})
		if err != nil {
			return 0, "", fmt.Errorf("unable to count activity executions: %w", err)
		}
		return resp.GetCount(), "activities", nil
	default:
		return 0, "", fmt.Errorf("unsupported delegated batch operation: %v", batchType)
	}
}

// checkTargetNamespaceActive fails before the job is started if this cluster is not active for
// the target namespace. StartAdminBatchOperation rejects it too; checking here names both
// clusters and avoids prompting for a job that cannot run.
func checkTargetNamespaceActive(
	ctx context.Context,
	adminClient adminservice.AdminServiceClient,
	workflowClient workflowservice.WorkflowServiceClient,
	nsName string,
) error {
	nsResp, err := workflowClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: nsName,
	})
	if err != nil {
		return fmt.Errorf("unable to describe namespace %q: %w", nsName, err)
	}
	// A local namespace is active in every cluster it exists in.
	if !nsResp.GetIsGlobalNamespace() {
		return nil
	}

	clusterResp, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		return fmt.Errorf("unable to describe cluster: %w", err)
	}

	activeCluster := nsResp.GetReplicationConfig().GetActiveClusterName()
	if activeCluster != clusterResp.GetClusterName() {
		return fmt.Errorf(
			"namespace %q is active in cluster %q, but this cluster is %q: a batch operation must be started in the active cluster",
			nsName, activeCluster, clusterResp.GetClusterName())
	}
	return nil
}

// delegatedBatchType maps the --batch-type value to the batch operation the admin API delegates.
// The operation itself needs no parameters here: identity and reason travel on the envelope.
func delegatedBatchType(batchType string) (enumspb.BatchOperationType, error) {
	switch batchType {
	case batchTypeTerminateWorkflows:
		return enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW, nil
	case batchTypeTerminateActivities:
		return enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY, nil
	default:
		return enumspb.BATCH_OPERATION_TYPE_UNSPECIFIED,
			fmt.Errorf("unknown batch type %q, expected one of: %s", batchType, strings.Join(batchTypes, ", "))
	}
}
