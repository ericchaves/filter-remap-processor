package utils

import "time"

type Ticker interface {
	Start(duration time.Duration)
	onTick()
	Stop()
}

type ProcessorTicker struct {
	Ticker     *time.Ticker
	OnTickFunc func()
	stopChan   chan struct{}
}

var _ Ticker = (*ProcessorTicker)(nil)

func (t *ProcessorTicker) Start(duration time.Duration) {
	t.Ticker = time.NewTicker(duration)
	t.stopChan = make(chan struct{})
	go func() {
		for {
			select {
			case <-t.Ticker.C:
				t.onTick()
			case <-t.stopChan:
				t.Ticker.Stop()
				return
			}
		}
	}()
}

func (t *ProcessorTicker) onTick() {
	if t.OnTickFunc != nil {
		t.OnTickFunc()
	}
}

func (t *ProcessorTicker) Stop() {
	if t.stopChan == nil {
		return
	}
	// Use select with default to avoid double close panic
	select {
	case <-t.stopChan:
		// Already closed
		return
	default:
		close(t.stopChan)
		t.Ticker.Stop()
	}
}
