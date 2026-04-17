package clock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestTimeSkippingTimeSource_Now_NoSkipping(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := clock.NewTimeSkippingTimeSource(base)

	require.Equal(t, time.Unix(100, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Now_NilInfo(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := clock.NewTimeSkippingTimeSource(base)

	ts.SetTimeSkippingInfo(nil)

	require.Equal(t, time.Unix(100, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Now_WithNoDuration(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := clock.NewTimeSkippingTimeSource(base)

	ts.SetTimeSkippingInfo(&persistencespb.TimeSkippingInfo{})

	require.Equal(t, time.Unix(100, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Now_WithSkipping(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := clock.NewTimeSkippingTimeSource(base)

	ts.SetTimeSkippingInfo(&persistencespb.TimeSkippingInfo{
		AccumulatedSkippedDuration: durationpb.New(10 * time.Second),
	})

	require.Equal(t, time.Unix(110, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Now_BaseAdvancesWithSkipping(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := clock.NewTimeSkippingTimeSource(base)

	ts.SetTimeSkippingInfo(&persistencespb.TimeSkippingInfo{
		AccumulatedSkippedDuration: durationpb.New(5 * time.Second),
	})

	base.Advance(10 * time.Second)
	require.Equal(t, time.Unix(115, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Since_DelegatesToBase(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := clock.NewTimeSkippingTimeSource(base)

	// Skipping offset does not affect Since — it delegates to the base.
	ts.SetTimeSkippingInfo(&persistencespb.TimeSkippingInfo{
		AccumulatedSkippedDuration: durationpb.New(50 * time.Second),
	})

	past := time.Unix(90, 0)
	require.Equal(t, 10*time.Second, ts.Since(past))
}

func TestTimeSkippingTimeSource_AfterFunc_DelegatesToBase(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSource(base)

	fired := false
	ts.AfterFunc(time.Second, func() { fired = true })

	require.False(t, fired)
	base.Advance(time.Second)
	require.True(t, fired)
}

func TestTimeSkippingTimeSource_NewTimer_DelegatesToBase(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSource(base)

	ch, _ := ts.NewTimer(time.Second)

	select {
	case <-ch:
		t.Fatal("timer should not fire before deadline")
	default:
	}

	base.Advance(time.Second)

	select {
	case <-ch:
		// fired as expected
	default:
		t.Fatal("timer should have fired after deadline")
	}
}
