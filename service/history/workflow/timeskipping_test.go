package workflow

import (
	"time"

	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/durationpb"
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
		tsc, initialSkip := propagateTimeSkippingToNextRun(newSource())
		s.Require().NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.Equal(3*time.Hour, tsc.GetFastForward().AsDuration())
		s.True(tsc.GetDisableChildPropagation())
		s.Equal(time.Hour, initialSkip.AsDuration())
	})

	s.Run("child gets no propagation when DisableChildPropagation is set", func() {
		tsc, initialSkip := propagateTimeSkippingToChild(newSource())
		s.Nil(tsc, "DisableChildPropagation=true → no config propagated to the child")
		s.Nil(initialSkip, "DisableChildPropagation=true → no virtual time propagated to the child")
	})

	s.Run("child clears FastForward and inherits Enabled when propagation is allowed", func() {
		src := newSource()
		src.TimeSkippingInfo.Config.DisableChildPropagation = false
		tsc, initialSkip := propagateTimeSkippingToChild(src)
		s.Require().NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.Nil(tsc.GetFastForward(), "FastForward never cascades into children")
		s.Equal(time.Hour, initialSkip.AsDuration())
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
