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
		cfg, initialSkip := propagateTimeSkippingForExecutionChain(newSource())
		s.Require().NotNil(cfg)
		s.True(cfg.GetEnabled())
		s.Equal(3*time.Hour, cfg.GetFastForward().AsDuration())
		s.True(cfg.GetDisableChildPropagation())
		s.Require().NotNil(initialSkip)
		s.Equal(time.Hour, initialSkip.AsDuration())
	})

	s.Run("child drops config when DisableChildPropagation is set, still propagates virtual time", func() {
		cfg, initialSkip := propagateTimeSkippingForChild(newSource())
		s.Nil(cfg, "DisableChildPropagation=true → no config propagated to the child")
		s.Require().NotNil(initialSkip)
		s.Equal(time.Hour, initialSkip.AsDuration(),
			"virtual time is always propagated, even when config propagation is disabled")
	})

	s.Run("child clears FastForward and inherits Enabled when propagation is allowed", func() {
		src := newSource()
		src.TimeSkippingInfo.Config.DisableChildPropagation = false
		cfg, _ := propagateTimeSkippingForChild(src)
		s.Require().NotNil(cfg)
		s.True(cfg.GetEnabled())
		s.Nil(cfg.GetFastForward(), "FastForward never cascades into children")
	})

	s.Run("execution-chain snapshot does not mutate the source config", func() {
		src := newSource()
		cfg, _ := propagateTimeSkippingForExecutionChain(src)
		s.Require().NotNil(cfg)
		cfg.Enabled = false
		cfg.FastForward = nil
		s.True(src.GetTimeSkippingInfo().GetConfig().GetEnabled(), "source Enabled must not be mutated")
		s.Equal(3*time.Hour, src.GetTimeSkippingInfo().GetConfig().GetFastForward().AsDuration(),
			"source FastForward must not be mutated")
	})
}
