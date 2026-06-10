package workflow

import (
	"time"

	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *mutableStateSuite) TestSnapshotTimeSkippingInfo_ContinuationVsChild() {
	newSource := func() *persistencespb.WorkflowExecutionInfo {
		return &persistencespb.WorkflowExecutionInfo{
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				Config: &workflowpb.TimeSkippingConfig{
					Enabled:                 true,
					FastForward:             durationpb.New(3 * time.Hour),
					DisableChildPropagation: true},
				AccumulatedSkippedDuration: durationpb.New(time.Hour),
			},
		}
	}

	s.Run("continuation keeps FastForward and Enabled, ignores DisableChildPropagation", func() {
		tsc, stateProp := propagateTimeSkippingToNextRun(newSource())
		s.Require().NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.Equal(3*time.Hour, tsc.GetFastForward().AsDuration())
		s.True(tsc.GetDisableChildPropagation())
		s.Require().NotNil(stateProp)
		s.Equal(time.Hour, stateProp.GetInitialSkippedDuration().AsDuration())
		s.Nil(stateProp.GetFastForwardTargetTime(),
			"no active fast-forward on source → no target time propagated")
	})

	s.Run("continuation propagates active FastForwardTargetTime", func() {
		src := newSource()
		ffTarget := time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC)
		src.TimeSkippingInfo.FastForward = &persistencespb.FastForwardInfo{
			TargetTime: timestamppb.New(ffTarget),
			HasReached: false,
		}
		_, stateProp := propagateTimeSkippingToNextRun(src)
		s.Require().NotNil(stateProp)
		s.Equal(ffTarget, stateProp.GetFastForwardTargetTime().AsTime(),
			"active fast-forward target must be propagated to next run")
	})

	s.Run("continuation does not propagate already-reached FastForward", func() {
		src := newSource()
		src.TimeSkippingInfo.FastForward = &persistencespb.FastForwardInfo{
			TargetTime: timestamppb.New(time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC)),
			HasReached: true,
		}
		_, stateProp := propagateTimeSkippingToNextRun(src)
		s.Require().NotNil(stateProp)
		s.Nil(stateProp.GetFastForwardTargetTime(),
			"reached fast-forward must not be re-propagated")
	})

	s.Run("child gets no propagation when DisableChildPropagation is set", func() {
		tsc, stateProp := propagateTimeSkippingToChild(newSource())
		s.Nil(tsc, "DisableChildPropagation=true → no config propagated to the child")
		s.Nil(stateProp, "DisableChildPropagation=true → no virtual time propagated to the child")
	})

	s.Run("child clears FastForward and inherits Enabled when propagation is allowed", func() {
		src := newSource()
		src.TimeSkippingInfo.Config.DisableChildPropagation = false
		src.TimeSkippingInfo.FastForward = &persistencespb.FastForwardInfo{
			TargetTime: timestamppb.New(time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC)),
			HasReached: false,
		}
		tsc, stateProp := propagateTimeSkippingToChild(src)
		s.Require().NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.Nil(tsc.GetFastForward(), "FastForward never cascades into children")
		s.Require().NotNil(stateProp)
		s.Equal(time.Hour, stateProp.GetInitialSkippedDuration().AsDuration())
		s.Nil(stateProp.GetFastForwardTargetTime(),
			"FastForwardTargetTime never cascades into children")
	})

	s.Run("execution-chain snapshot does not mutate the source config", func() {
		src := newSource()
		tsc, _ := propagateTimeSkippingToNextRun(src)
		s.Require().NotNil(tsc)
		tsc.Enabled = false
		tsc.FastForward = nil
		s.True(src.GetTimeSkippingInfo().GetConfig().GetEnabled(), "source Enabled must not be mutated")
		s.Equal(3*time.Hour, src.GetTimeSkippingInfo().GetConfig().GetFastForward().AsDuration(),
			"source FastForward must not be mutated")
	})
}
