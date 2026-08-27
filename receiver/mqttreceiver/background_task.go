package mqttreceiver

import (
	"context"
	"sync"
)

type backgroundTask struct {
	mu      sync.Mutex
	cancel  context.CancelFunc
	stopped bool
	wg      sync.WaitGroup
}

func newBackgroundTask() *backgroundTask {
	return &backgroundTask{}
}

func (b *backgroundTask) Restart(f func(context.Context)) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stopped {
		return
	}

	if b.cancel != nil {
		b.cancel()
		b.wg.Wait()
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		f(ctx)
	}()
}

func (b *backgroundTask) Stop() {
	b.mu.Lock()
	if !b.stopped {
		b.stopped = true
		if b.cancel != nil {
			b.cancel()
		}
	}
	b.mu.Unlock()

	b.wg.Wait()
}
