package tests

// This file contains two tests that examine how history_tree and history_node
// rows in SQLite evolve through a workflow reset + deletion lifecycle.
//
// Root cause of the known bug (history_manager.go:DeleteHistoryBranch):
// When workflow A is deleted and workflow B (a reset of A) still exists and references
// A's branch as an ancestor, deleteRanges is computed as empty because A's branch is
// fully "used" by B. DeleteHistoryBranch then:
//   - deletes history_tree[A]  ✓
//   - skips history_node[A rows] ✗  (empty deleteRanges loop)
//
// After history_tree[A] is gone, the history scavenger can no longer find A's history_node
// rows — it only iterates history_tree. Those rows are permanently orphaned.

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite" // register sqlite_temporal driver
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type HistoryNodeCleanupSuite struct {
	testcore.FunctionalTestBase
}

func TestHistoryNodeCleanupSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(HistoryNodeCleanupSuite))
}

// SetupSuite overrides the default in-memory SQLite with a file-based database.
// File-based SQLite is required so that logRawSQLiteCounts can open a second
// connection and query table row counts and byte sizes directly.
func (s *HistoryNodeCleanupSuite) SetupSuite() {
	s.SetupSuiteWithCluster(testcore.WithSharedCluster())
}

// TestHistoryTablesStateAfterResetAndDelete runs a realistic workflow (5 sequential
// activities), resets it to create run B (which also completes 5 activities), then
// force-deletes both runs using the same code path as DeleteHistoryEventTask.
//
// At each deletion step the test logs the exact state of history_tree and history_node —
// including row counts and serialized byte sizes — so the full lifecycle is visible.
// Raw SQLite queries provide ground-truth row counts and exact on-disk sizes.
//
// Run with -v to see the per-step table logs:
//
//	go test ./tests/ -run TestHistoryNodeCleanupSuite/TestHistoryTablesStateAfterResetAndDelete -v
func (s *HistoryNodeCleanupSuite) TestHistoryTablesStateAfterResetAndDelete() {
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	shardID := common.WorkflowIDToHistoryShard(
		s.NamespaceID().String(),
		tv.WorkflowID(),
		s.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)
	execMgr := s.GetTestCluster().TestBase().ExecutionManager
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())

	// ── Step 1: start and complete run A (5 sequential activities) ───────────
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    s.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	})
	s.NoError(err)
	runIDA := startResp.RunId

	s.completeWorkflowWithActivities(tv, poller, 5)

	// Capture A's branch token (tree_id = runIDA for the whole tree).
	branchTokenA := s.captureCurrentBranchToken(ctx, tv.WorkflowID(), runIDA)

	// Find the first WorkflowTaskCompleted event to use as the reset point.
	// Resetting here means B inherits A's very first events (WorkflowExecutionStarted
	// + WorkflowTaskScheduled + WorkflowTaskStarted) and forks from there.
	var resetEventID int64
	hist := s.SdkClient().GetWorkflowHistory(ctx, tv.WorkflowID(), runIDA, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			resetEventID = event.EventId
			break
		}
	}
	s.NotZero(resetEventID)

	// ── Step 2: reset A → run B (also runs 5 activities) ────────────────────
	// B's history_tree row lists A's branch_id as an ancestor; tree_id is shared.
	resetResp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runIDA},
		Reason:                    "test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: resetEventID,
	})
	s.NoError(err)
	runIDB := resetResp.RunId

	tvB := tv.WithRunID(runIDB)
	s.completeWorkflowWithActivities(tvB, poller, 5)

	// Capture B's branch token. Both A and B share tree_id = runIDA.
	branchTokenB := s.captureCurrentBranchToken(ctx, tv.WorkflowID(), runIDB)

	// treeID = runIDA: the original run_id IS the tree_id for all branches in this history tree.
	treeID := runIDA

	// ── BEFORE any deletion ───────────────────────────────────────────────────
	// Expected: 2 history_tree rows, many history_node rows for both branches.
	s.logHistoryTableState(ctx, "BEFORE any deletion", shardID, execMgr, s.NamespaceID().String(), tv.WorkflowID(), treeID, branchTokenA, branchTokenB)

	// ── Step 3: force-delete run A ────────────────────────────────────────────
	// Same 4-stage deletion path as DeleteHistoryEventTask / retention.
	// B still exists → DeleteHistoryBranch for A finds B in history_tree.
	// Result: history_tree[A] deleted; A's shared prefix nodes survive (used by B);
	//         A's exclusive tail nodes are deleted.
	_, err = s.AdminClient().DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runIDA},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.waitForMutableStateGone(ctx, shardID, execMgr, tv.WorkflowID(), runIDA)

	// ── AFTER deleting A (B still alive) ─────────────────────────────────────
	// Expected: history_tree[A] is gone; history_tree[B] remains.
	// A's shared prefix nodes remain in history_node (B still needs them as ancestor).
	// A's exclusive tail nodes (past the fork point) have been deleted.
	s.logHistoryTableState(ctx, "AFTER deleting run A  (B still alive)", shardID, execMgr, s.NamespaceID().String(), tv.WorkflowID(), treeID, branchTokenA, branchTokenB)

	// ── Step 4: force-delete run B ────────────────────────────────────────────
	_, err = s.AdminClient().DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runIDB},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.waitForMutableStateGone(ctx, shardID, execMgr, tv.WorkflowID(), runIDB)

	// ── AFTER deleting both runs ──────────────────────────────────────────────
	// Expected: 0 history_tree rows, 0 history_node rows, 0 bytes for this tree.
	s.logHistoryTableState(ctx, "AFTER deleting both runs", shardID, execMgr, s.NamespaceID().String(), tv.WorkflowID(), treeID, branchTokenA, branchTokenB)

	// ── Assertions ────────────────────────────────────────────────────────────
	// After both runs are fully deleted, no history_node rows should remain for
	// either branch. A NotFound error is also acceptable.
	for _, tc := range []struct {
		label string
		token []byte
	}{
		{"run A (original)", branchTokenA},
		{"run B (reset)", branchTokenB},
	} {
		resp, err := execMgr.ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
			ShardID:     shardID,
			BranchToken: tc.token,
			MinEventID:  common.FirstEventID,
			MaxEventID:  common.EndEventID,
			PageSize:    1000,
		})
		if err == nil {
			s.Empty(resp.HistoryEvents,
				"history_node rows for %s should be gone after both runs are deleted", tc.label)
		}
		// A NotFound/InvalidArgument error is acceptable: it means the branch is gone.
	}
}

// completeWorkflowWithActivities drives a workflow that has already been started
// through numActivities sequential activities and then completes the workflow.
// Each activity is scheduled one at a time: one workflow task schedules the next
// activity, the activity is polled and completed, then the next workflow task either
// schedules the following activity or (on the last round) completes the workflow.
func (s *HistoryNodeCleanupSuite) completeWorkflowWithActivities(
	tv *testvars.TestVars,
	poller *taskpoller.TaskPoller,
	numActivities int,
) {
	activitiesScheduled := 0

	wtHandler := func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if activitiesScheduled < numActivities {
			activitiesScheduled++
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             fmt.Sprintf("act-%d", activitiesScheduled),
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
							StartToCloseTimeout:    durationpb.New(10 * time.Second),
							Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{
								{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(fmt.Sprintf("%q", uuid.NewString()))},
								{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(fmt.Sprintf("%q", uuid.NewString()))},
								{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(fmt.Sprintf("%q", uuid.NewString()))},
								{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(fmt.Sprintf("%q", uuid.NewString()))},
							}},
						},
					},
				}},
			}, nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
			}},
		}, nil
	}

	// First workflow task: schedules activity 1.
	_, err := poller.PollAndHandleWorkflowTask(tv, wtHandler)
	s.NoError(err)

	for i := 0; i < numActivities; i++ {
		// Complete activity i+1.
		_, err = poller.PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
		s.NoError(err)
		// Next workflow task: either schedules activity i+2 or completes the workflow.
		_, err = poller.PollAndHandleWorkflowTask(tv, wtHandler)
		s.NoError(err)
	}
}

// captureCurrentBranchToken extracts the current branch token from a workflow's mutable state.
func (s *HistoryNodeCleanupSuite) captureCurrentBranchToken(ctx context.Context, workflowID, runID string) []byte {
	descResp, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	vh := descResp.GetDatabaseMutableState().GetExecutionInfo().GetVersionHistories()
	currentVH, err := versionhistory.GetCurrentVersionHistory(vh)
	s.NoError(err)
	token := currentVH.GetBranchToken()
	s.NotEmpty(token)
	return token
}

// waitForMutableStateGone polls until GetWorkflowExecution returns NotFound for runID.
func (s *HistoryNodeCleanupSuite) waitForMutableStateGone(ctx context.Context, shardID int32, execMgr persistence.ExecutionManager, workflowID, runID string) {
	s.Eventually(func() bool {
		_, err := execMgr.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID:     shardID,
			NamespaceID: s.NamespaceID().String(),
			WorkflowID:  workflowID,
			RunID:       runID,
			ArchetypeID: chasm.WorkflowArchetypeID,
		})
		return common.IsNotFoundError(err)
	}, 10*time.Second, 100*time.Millisecond,
		"timed out waiting for mutable state of run %s to be deleted", runID)
}

// logHistoryTableState prints the state of history_tree and history_node for the
// given tree at a given point in time.
//
// Uses two complementary views:
//  1. ExecutionManager.GetAllHistoryTreeBranches  → history_tree rows (logical)
//  2. ExecutionManager.ReadHistoryBranch          → event count + serialized bytes per branch
//  3. Raw SQLite queries                          → exact row counts and SUM(length(data))
func (s *HistoryNodeCleanupSuite) logHistoryTableState(
	ctx context.Context,
	label string,
	shardID int32,
	execMgr persistence.ExecutionManager,
	namespaceID string,
	workflowID string,
	treeID string,
	branchTokens ...[]byte,
) {
	s.T().Logf("══ %s ══", label)

	// ── history_tree: one row per branch ──────────────────────────────────────
	var treeRows []persistence.HistoryBranchDetail
	req := &persistence.GetAllHistoryTreeBranchesRequest{PageSize: 100}
	for {
		resp, err := execMgr.GetAllHistoryTreeBranches(ctx, req)
		if err != nil {
			s.T().Logf("  history_tree scan error: %v", err)
			break
		}
		for _, b := range resp.Branches {
			if b.BranchInfo.GetTreeId() == treeID {
				treeRows = append(treeRows, b)
			}
		}
		if len(resp.NextPageToken) == 0 {
			break
		}
		req.NextPageToken = resp.NextPageToken
	}
	s.T().Logf("  history_tree: %d row(s) for tree_id=%s…", len(treeRows), treeID[:8])
	for _, b := range treeRows {
		s.T().Logf("    branch_id=%-36s ancestors=%d fork_time=%v",
			b.BranchInfo.GetBranchId(),
			len(b.BranchInfo.GetAncestors()),
			b.ForkTime.AsTime().Format(time.RFC3339),
		)
		for _, anc := range b.BranchInfo.GetAncestors() {
			s.T().Logf("      ancestor branch_id=%-36s [node %d, %d)",
				anc.GetBranchId(), anc.GetBeginNodeId(), anc.GetEndNodeId())
		}
	}

	// ── history_node: events + serialized bytes per branch token ──────────────
	// ReadHistoryBranch reads one ancestor-range per page (the branch token encodes
	// an ancestor chain; each segment is a separate DB range). Paginate until nil
	// NextPageToken to accumulate all events and the total serialized byte count.
	for i, token := range branchTokens {
		var (
			allEvents     []*historypb.HistoryEvent
			totalSize     int
			nextPageToken []byte
			readErr       error
		)
		for {
			resp, err := execMgr.ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
				ShardID:       shardID,
				BranchToken:   token,
				MinEventID:    common.FirstEventID,
				MaxEventID:    common.EndEventID,
				PageSize:      1000,
				NextPageToken: nextPageToken,
			})
			if err != nil {
				readErr = err
				break
			}
			allEvents = append(allEvents, resp.HistoryEvents...)
			totalSize += resp.Size
			nextPageToken = resp.NextPageToken
			if len(nextPageToken) == 0 {
				break
			}
		}
		if readErr != nil && len(allEvents) == 0 {
			// Branch is gone — NotFound is expected after deletion.
			s.T().Logf("  branch[%d] history_node: gone (%v)", i, readErr)
		} else {
			first, last := int64(0), int64(0)
			if len(allEvents) > 0 {
				first = allEvents[0].EventId
				last = allEvents[len(allEvents)-1].EventId
			}
			s.T().Logf("  branch[%d] history_node: %d event(s) (event_id %d…%d)  serialized=%d B",
				i, len(allEvents), first, last, totalSize)
		}
	}

	// ── raw SQLite: ground-truth row counts + exact table sizes ───────────────
	s.logRawSQLiteCounts(shardID, namespaceID, workflowID, treeID)
}

// logRawSQLiteCounts opens the SQLite file used by the test cluster and queries
// exact row counts and SUM(length(data)) for executions, history_tree, and history_node.
// Skips if the backend is not file-based SQLite (e.g. Cassandra, or in-memory SQLite).
func (s *HistoryNodeCleanupSuite) logRawSQLiteCounts(shardID int32, namespaceID, workflowID, treeID string) {
	cfg := s.GetTestCluster().TestBase().DefaultTestCluster.Config()
	sqlCfg := cfg.DataStores[cfg.DefaultStore].SQL
	if sqlCfg == nil {
		return // not a SQL backend (e.g. Cassandra)
	}
	if sqlCfg.ConnectAttributes["mode"] == "memory" {
		s.T().Logf("  [sqlite] skipping raw counts: in-memory database (mode=memory)")
		return
	}

	// Build a DSN pointing at the same file the server has open.
	attrs := []string{"cache=shared"}
	for k, v := range sqlCfg.ConnectAttributes {
		if k != "cache" {
			attrs = append(attrs, fmt.Sprintf("%s=%s", k, v))
		}
	}
	dsn := fmt.Sprintf("file:%s?%s", sqlCfg.DatabaseName, strings.Join(attrs, "&"))
	db, err := gosql.Open("sqlite_temporal", dsn)
	if err != nil {
		s.T().Logf("  [sqlite] open error: %v", err)
		return
	}
	defer func() { _ = db.Close() }()

	nsIDBytes, err := primitives.ParseUUID(namespaceID)
	if err != nil {
		s.T().Logf("  [sqlite] parse namespace_id error: %v", err)
		return
	}
	treeIDBytes, err := primitives.ParseUUID(treeID)
	if err != nil {
		s.T().Logf("  [sqlite] parse tree_id error: %v", err)
		return
	}
	nsArg := []byte(nsIDBytes)
	treeArg := []byte(treeIDBytes)

	// executions: row count + total serialized bytes, one row per live run.
	var execRowCount int
	var execBytes int64
	if err := db.QueryRow(
		`SELECT COUNT(*), COALESCE(SUM(length(data)), 0)
		   FROM executions WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ?`,
		shardID, nsArg, workflowID,
	).Scan(&execRowCount, &execBytes); err != nil {
		s.T().Logf("  [sqlite] executions query error: %v", err)
	} else {
		s.T().Logf("  [sqlite] executions:   %d row(s)  %d B", execRowCount, execBytes)
	}
	// Per-run breakdown.
	execRows, err := db.Query(
		`SELECT hex(run_id), COALESCE(length(data), 0)
		   FROM executions WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ?
		  ORDER BY run_id`,
		shardID, nsArg, workflowID,
	)
	if err != nil {
		s.T().Logf("  [sqlite] executions per-run query error: %v", err)
	} else {
		defer func() { _ = execRows.Close() }()
		for execRows.Next() {
			var runHex string
			var runBytes int64
			if err := execRows.Scan(&runHex, &runBytes); err == nil {
				s.T().Logf("    run_id(hex)=%s  bytes=%d", runHex, runBytes)
			}
		}
	}

	// history_tree: row count + total serialized bytes.
	var treeRowCount int
	var treeBytes int64
	if err := db.QueryRow(
		`SELECT COUNT(*), COALESCE(SUM(length(data)), 0)
		   FROM history_tree WHERE shard_id = ? AND tree_id = ?`,
		shardID, treeArg,
	).Scan(&treeRowCount, &treeBytes); err != nil {
		s.T().Logf("  [sqlite] history_tree query error: %v", err)
	} else {
		s.T().Logf("  [sqlite] history_tree:  %d row(s)  %d B", treeRowCount, treeBytes)
	}

	// history_node: row count + total serialized bytes.
	var nodeRowCount int
	var nodeBytes int64
	if err := db.QueryRow(
		`SELECT COUNT(*), COALESCE(SUM(length(data)), 0)
		   FROM history_node WHERE shard_id = ? AND tree_id = ?`,
		shardID, treeArg,
	).Scan(&nodeRowCount, &nodeBytes); err != nil {
		s.T().Logf("  [sqlite] history_node query error: %v", err)
	} else {
		s.T().Logf("  [sqlite] history_node:  %d row(s)  %d B", nodeRowCount, nodeBytes)
	}

	// Per-branch breakdown: rows, node_id range, bytes.
	nodeRows, err := db.Query(
		`SELECT hex(branch_id), COUNT(*), MIN(node_id), MAX(node_id), COALESCE(SUM(length(data)), 0)
		   FROM history_node
		  WHERE shard_id = ? AND tree_id = ?
		  GROUP BY branch_id
		  ORDER BY MIN(node_id)`,
		shardID, treeArg,
	)
	if err != nil {
		s.T().Logf("  [sqlite] history_node per-branch query error: %v", err)
		return
	}
	defer func() { _ = nodeRows.Close() }()
	for nodeRows.Next() {
		var branchHex string
		var cnt int
		var minNode, maxNode, branchBytes int64
		if err := nodeRows.Scan(&branchHex, &cnt, &minNode, &maxNode, &branchBytes); err != nil {
			continue
		}
		s.T().Logf("    branch(hex)=%s  rows=%d  node_id=[%d,%d]  bytes=%d",
			branchHex, cnt, minNode, maxNode, branchBytes)
	}
}
