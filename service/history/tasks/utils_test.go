package tasks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTimerTaskEventID_TimeSkippingTimerTask(t *testing.T) {
	t.Parallel()

	// Time-skipping tasks are validated via Stamp/Version, not an event ID, so they report
	// no event ID (ok == false).
	task := &TimeSkippingTimerTask{Stamp: 3}

	gotEventID, ok := GetTimerTaskEventID(task)
	require.False(t, ok)
	require.Equal(t, int64(0), gotEventID)
}
