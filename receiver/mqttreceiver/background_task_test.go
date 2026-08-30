package mqttreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_backgroundTask(t *testing.T) {
	task := newBackgroundTask()

	firstStarted := make(chan struct{})
	firstStopped := make(chan struct{})
	task.Restart(func(ctx context.Context) {
		close(firstStarted)
		<-ctx.Done()
		close(firstStopped)
	})
	require.Eventually(t, isClosed(firstStarted), time.Second, time.Millisecond)

	secondStarted := make(chan struct{})
	secondStopped := make(chan struct{})
	task.Restart(func(ctx context.Context) {
		close(secondStarted)
		<-ctx.Done()
		close(secondStopped)
	})
	require.True(t, isClosed(firstStopped)(), "Restart should stop the previous task before returning")
	require.Eventually(t, isClosed(secondStarted), time.Second, time.Millisecond)

	task.Stop()
	require.True(t, isClosed(secondStopped)(), "Stop should wait for the running task")

	restarted := make(chan struct{})
	task.Restart(func(context.Context) {
		close(restarted)
	})
	require.Never(t, isClosed(restarted), 50*time.Millisecond, time.Millisecond, "Restart should do nothing after Stop")
}

func isClosed(ch <-chan struct{}) func() bool {
	return func() bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
}
