package utils

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessorTicker(t *testing.T) {
	t.Run("basic tick functionality", func(t *testing.T) {
		tickCount := atomic.Int32{}
		ticker := &ProcessorTicker{
			OnTickFunc: func() {
				tickCount.Add(1)
			},
		}

		ticker.Start(10 * time.Millisecond)
		defer ticker.Stop()

		// Wait for a few ticks
		time.Sleep(35 * time.Millisecond)

		// Should have ticked at least 2-3 times
		count := tickCount.Load()
		assert.GreaterOrEqual(t, count, int32(2), "Expected at least 2 ticks")
		assert.LessOrEqual(t, count, int32(5), "Expected no more than 5 ticks")
	})

	t.Run("stop ticker", func(t *testing.T) {
		tickCount := atomic.Int32{}
		ticker := &ProcessorTicker{
			OnTickFunc: func() {
				tickCount.Add(1)
			},
		}

		ticker.Start(10 * time.Millisecond)
		time.Sleep(25 * time.Millisecond)

		countBeforeStop := tickCount.Load()
		ticker.Stop()

		// Wait a bit to ensure no more ticks
		time.Sleep(30 * time.Millisecond)
		countAfterStop := tickCount.Load()

		// Count should not increase after stop
		assert.Equal(t, countBeforeStop, countAfterStop, "Ticker should not tick after Stop()")
	})

	t.Run("stop before start", func(t *testing.T) {
		ticker := &ProcessorTicker{
			OnTickFunc: func() {},
		}

		// Should not panic
		assert.NotPanics(t, func() {
			ticker.Stop()
		})
	})

	t.Run("multiple stops", func(t *testing.T) {
		ticker := &ProcessorTicker{
			OnTickFunc: func() {},
		}

		ticker.Start(10 * time.Millisecond)
		ticker.Stop()

		// Should not panic on second stop
		assert.NotPanics(t, func() {
			ticker.Stop()
		})
	})

	t.Run("long tick interval", func(t *testing.T) {
		tickCount := atomic.Int32{}
		ticker := &ProcessorTicker{
			OnTickFunc: func() {
				tickCount.Add(1)
			},
		}

		ticker.Start(100 * time.Millisecond)
		defer ticker.Stop()

		// Wait less than the tick interval
		time.Sleep(50 * time.Millisecond)

		// Should not have ticked yet
		assert.Equal(t, int32(0), tickCount.Load())

		// Wait for the tick
		time.Sleep(60 * time.Millisecond)

		// Should have ticked once
		count := tickCount.Load()
		assert.GreaterOrEqual(t, count, int32(1))
	})

	t.Run("on tick function modification", func(t *testing.T) {
		var result string
		ticker := &ProcessorTicker{
			OnTickFunc: func() {
				result = "first"
			},
		}

		ticker.Start(10 * time.Millisecond)
		time.Sleep(15 * time.Millisecond)
		assert.Equal(t, "first", result)

		// Modify the OnTickFunc
		ticker.OnTickFunc = func() {
			result = "second"
		}

		time.Sleep(15 * time.Millisecond)
		ticker.Stop()

		assert.Equal(t, "second", result)
	})

	t.Run("concurrent ticks", func(t *testing.T) {
		tickCount := atomic.Int32{}
		inProgress := atomic.Int32{}
		maxConcurrent := atomic.Int32{}

		ticker := &ProcessorTicker{
			OnTickFunc: func() {
				current := inProgress.Add(1)

				// Track max concurrent executions
				for {
					max := maxConcurrent.Load()
					if current <= max || maxConcurrent.CompareAndSwap(max, current) {
						break
					}
				}

				// Simulate some work
				time.Sleep(5 * time.Millisecond)

				inProgress.Add(-1)
				tickCount.Add(1)
			},
		}

		ticker.Start(3 * time.Millisecond)
		time.Sleep(30 * time.Millisecond)
		ticker.Stop()

		// We should have multiple ticks
		assert.Greater(t, tickCount.Load(), int32(2))

		// Since ticks can overlap, max concurrent might be > 1
		t.Logf("Max concurrent ticks: %d", maxConcurrent.Load())
	})

	t.Run("ticker interface compliance", func(t *testing.T) {
		var ticker Ticker = &ProcessorTicker{
			OnTickFunc: func() {},
		}

		require.NotNil(t, ticker)

		// Should not panic
		assert.NotPanics(t, func() {
			ticker.Start(10 * time.Millisecond)
			ticker.onTick()
			ticker.Stop()
		})
	})
}

func TestProcessorTickerEdgeCases(t *testing.T) {
	t.Run("nil OnTickFunc", func(t *testing.T) {
		ticker := &ProcessorTicker{}

		// Should panic when calling onTick with nil function
		ticker.Start(10 * time.Millisecond)
		defer ticker.Stop()

		// Give it time to potentially panic
		time.Sleep(20 * time.Millisecond)

		// If we get here without panic, test passes
		// (though ideally OnTickFunc should never be nil)
	})

	t.Run("very fast ticks", func(t *testing.T) {
		tickCount := atomic.Int32{}
		ticker := &ProcessorTicker{
			OnTickFunc: func() {
				tickCount.Add(1)
			},
		}

		// Very fast tick rate
		ticker.Start(1 * time.Millisecond)
		time.Sleep(50 * time.Millisecond)
		ticker.Stop()

		// Should have many ticks
		count := tickCount.Load()
		assert.Greater(t, count, int32(10), "Expected many ticks with 1ms interval")
		t.Logf("Tick count with 1ms interval over 50ms: %d", count)
	})

	t.Run("slow OnTickFunc", func(t *testing.T) {
		tickCount := atomic.Int32{}
		ticker := &ProcessorTicker{
			OnTickFunc: func() {
				time.Sleep(30 * time.Millisecond) // Slower than tick rate
				tickCount.Add(1)
			},
		}

		ticker.Start(10 * time.Millisecond)
		time.Sleep(50 * time.Millisecond)
		ticker.Stop()

		// Even though tick rate is fast, actual ticks are slower
		// due to the work being done
		count := tickCount.Load()
		t.Logf("Tick count with slow OnTickFunc: %d", count)
		assert.Greater(t, count, int32(0))
	})
}
