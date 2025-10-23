package filterremapprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestNewFilterRemapProcessor(t *testing.T) {
	t.Run("valid configuration", func(t *testing.T) {
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
		}

		ctx := context.Background()
		set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
		nextConsumer := consumertest.NewNop()

		processor, err := newFilterRemapProcessor(ctx, set, nextConsumer, cfg)
		require.NoError(t, err)
		require.NotNil(t, processor)

		assert.NotNil(t, processor.idToTrace)
		assert.NotNil(t, processor.forwardTraceChan)
		assert.Equal(t, cfg.MaxTraceRetention, processor.maxTraceRetention)
		assert.Equal(t, cfg.LastSpanTimeout, processor.lastSpanTimeout)
		assert.Equal(t, cfg.NumTraces, processor.maxNumTraces)
	})

	t.Run("with span conditions", func(t *testing.T) {
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
			Traces: TraceFilters{
				SpanConditions: []string{
					`attributes["http.status_code"] == 404`,
				},
			},
		}

		ctx := context.Background()
		set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
		nextConsumer := consumertest.NewNop()

		processor, err := newFilterRemapProcessor(ctx, set, nextConsumer, cfg)
		require.NoError(t, err)
		require.NotNil(t, processor)
		assert.NotNil(t, processor.skipSpanExpr)
	})

	t.Run("with invalid span conditions", func(t *testing.T) {
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
			Traces: TraceFilters{
				SpanConditions: []string{
					`invalid syntax {{`,
				},
			},
		}

		ctx := context.Background()
		set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
		nextConsumer := consumertest.NewNop()

		processor, err := newFilterRemapProcessor(ctx, set, nextConsumer, cfg)
		assert.Error(t, err)
		assert.Nil(t, processor)
	})
}

func TestProcessorCapabilities(t *testing.T) {
	cfg := &Config{
		ErrorMode:                    ottl.PropagateError,
		MaxTraceRetention:            5 * time.Minute,
		LastSpanTimeout:              10 * time.Second,
		NumTraces:                    10000,
		ExpectedNewTracesPerSec:      100,
		ExpectedAverageSpansPerTrace: 50,
	}

	ctx := context.Background()
	set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
	nextConsumer := consumertest.NewNop()

	processor, err := newFilterRemapProcessor(ctx, set, nextConsumer, cfg)
	require.NoError(t, err)

	capabilities := processor.Capabilities()
	assert.True(t, capabilities.MutatesData, "Processor should mutate data")
}

func TestProcessorStartStop(t *testing.T) {
	t.Run("start and stop", func(t *testing.T) {
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
		}

		ctx := context.Background()
		set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
		nextConsumer := consumertest.NewNop()

		processor, err := newFilterRemapProcessor(ctx, set, nextConsumer, cfg)
		require.NoError(t, err)

		err = processor.Start(ctx, componenttest.NewNopHost())
		require.NoError(t, err)

		err = processor.Shutdown(ctx)
		require.NoError(t, err)
	})

	t.Run("shutdown with flush", func(t *testing.T) {
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
			FlushOnShutdown:              true,
		}

		ctx := context.Background()
		set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
		mockConsumer := newMockTracesConsumer()

		processor, err := newFilterRemapProcessor(ctx, set, mockConsumer, cfg)
		require.NoError(t, err)

		err = processor.Start(ctx, componenttest.NewNopHost())
		require.NoError(t, err)

		// Add a trace
		traceID := newTraceID(1)
		traces := createSimpleTrace(traceID)
		err = processor.ConsumeTraces(ctx, traces)
		require.NoError(t, err)

		// Shutdown should flush
		err = processor.Shutdown(ctx)
		require.NoError(t, err)

		// Give some time for async operations
		time.Sleep(50 * time.Millisecond)

		// Should have forwarded the trace
		assert.Greater(t, mockConsumer.getTraceCount(), 0)
	})
}

func TestConsumeTraces(t *testing.T) {
	t.Run("simple trace", func(t *testing.T) {
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
		}

		ctx := context.Background()
		set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
		nextConsumer := consumertest.NewNop()

		processor, err := newFilterRemapProcessor(ctx, set, nextConsumer, cfg)
		require.NoError(t, err)

		traceID := newTraceID(1)
		traces := createSimpleTrace(traceID)

		err = processor.ConsumeTraces(ctx, traces)
		require.NoError(t, err)

		// Verify trace was added to the map
		assert.Equal(t, uint64(1), processor.numTracesOnMap.Load())
	})

	t.Run("multiple traces", func(t *testing.T) {
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
		}

		ctx := context.Background()
		set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
		nextConsumer := consumertest.NewNop()

		processor, err := newFilterRemapProcessor(ctx, set, nextConsumer, cfg)
		require.NoError(t, err)

		// Add multiple traces
		for i := byte(1); i <= 5; i++ {
			traceID := newTraceID(i)
			traces := createSimpleTrace(traceID)
			err = processor.ConsumeTraces(ctx, traces)
			require.NoError(t, err)
		}

		// Should have 5 traces in memory
		assert.Equal(t, uint64(5), processor.numTracesOnMap.Load())
	})

	t.Run("same trace multiple batches", func(t *testing.T) {
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
		}

		ctx := context.Background()
		set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
		nextConsumer := consumertest.NewNop()

		processor, err := newFilterRemapProcessor(ctx, set, nextConsumer, cfg)
		require.NoError(t, err)

		traceID := newTraceID(1)

		// Send first batch of spans
		traces1 := createTestTraces([]ptrace.Span{
			createTestSpan(newSpanID(1), pcommon.SpanID{}, traceID, "root"),
		})
		err = processor.ConsumeTraces(ctx, traces1)
		require.NoError(t, err)

		// Send second batch with same trace ID
		traces2 := createTestTraces([]ptrace.Span{
			createTestSpan(newSpanID(2), newSpanID(1), traceID, "child"),
		})
		err = processor.ConsumeTraces(ctx, traces2)
		require.NoError(t, err)

		// Should still only have 1 trace
		assert.Equal(t, uint64(1), processor.numTracesOnMap.Load())

		// Get the trace and verify span count
		tData, ok := processor.idToTrace.Get(traceID)
		require.True(t, ok)
		assert.Equal(t, int64(2), tData.SpanCount.Load())
	})
}

func TestFactoryCreation(t *testing.T) {
	t.Run("create default config", func(t *testing.T) {
		factory := NewFactory()
		require.NotNil(t, factory)

		cfg := factory.CreateDefaultConfig()
		require.NotNil(t, cfg)

		// Verify it's the right type
		_, ok := cfg.(*Config)
		assert.True(t, ok)
	})

	t.Run("create traces processor", func(t *testing.T) {
		factory := NewFactory()
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
		}

		ctx := context.Background()
		set := processor.Settings{
			ID:                component.NewIDWithName(component.MustNewType("filter_remap"), "test"),
			TelemetrySettings: componenttest.NewNopTelemetrySettings(),
			BuildInfo:         component.NewDefaultBuildInfo(),
		}
		nextConsumer := consumertest.NewNop()

		tp, err := factory.CreateTraces(ctx, set, cfg, nextConsumer)
		require.NoError(t, err)
		require.NotNil(t, tp)

		// Verify the processor can start
		err = tp.Start(ctx, componenttest.NewNopHost())
		require.NoError(t, err)

		// Verify the processor can shutdown
		err = tp.Shutdown(ctx)
		require.NoError(t, err)
	})
}

func TestDropRootSpans(t *testing.T) {
	t.Run("drop root spans false", func(t *testing.T) {
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
			DropRootSpans:                false,
			Traces: TraceFilters{
				SpanConditions: []string{`name == "child"`},
			},
		}

		ctx := context.Background()
		set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
		nextConsumer := consumertest.NewNop()

		processor, err := newFilterRemapProcessor(ctx, set, nextConsumer, cfg)
		require.NoError(t, err)

		traceID := newTraceID(1)
		rootSpan := createTestSpan(newSpanID(1), pcommon.SpanID{}, traceID, "root")
		childSpan := createTestSpan(newSpanID(2), newSpanID(1), traceID, "child")

		// Root span should not be filtered even though it could match a condition
		// because DropRootSpans is false
		shouldDrop, err := processor.shouldFilterSpan(
			rootSpan,
			pcommon.NewInstrumentationScope(),
			pcommon.NewResource(),
			ptrace.NewScopeSpans(),
			ptrace.NewResourceSpans(),
		)
		require.NoError(t, err)
		assert.False(t, shouldDrop, "Root span should not be dropped when DropRootSpans is false")

		// Child span should be filtered
		shouldDrop, err = processor.shouldFilterSpan(
			childSpan,
			pcommon.NewInstrumentationScope(),
			pcommon.NewResource(),
			ptrace.NewScopeSpans(),
			ptrace.NewResourceSpans(),
		)
		require.NoError(t, err)
		assert.True(t, shouldDrop, "Child span matching condition should be dropped")
	})

	t.Run("drop root spans true", func(t *testing.T) {
		cfg := &Config{
			ErrorMode:                    ottl.PropagateError,
			MaxTraceRetention:            5 * time.Minute,
			LastSpanTimeout:              10 * time.Second,
			NumTraces:                    10000,
			ExpectedNewTracesPerSec:      100,
			ExpectedAverageSpansPerTrace: 50,
			DropRootSpans:                true,
			Traces: TraceFilters{
				SpanConditions: []string{`name == "root"`},
			},
		}

		ctx := context.Background()
		set := processortest.NewNopSettings(component.MustNewType("filter_remap"))
		nextConsumer := consumertest.NewNop()

		processor, err := newFilterRemapProcessor(ctx, set, nextConsumer, cfg)
		require.NoError(t, err)

		traceID := newTraceID(1)
		rootSpan := createTestSpan(newSpanID(1), pcommon.SpanID{}, traceID, "root")

		// Root span should be filtered when DropRootSpans is true and condition matches
		shouldDrop, err := processor.shouldFilterSpan(
			rootSpan,
			pcommon.NewInstrumentationScope(),
			pcommon.NewResource(),
			ptrace.NewScopeSpans(),
			ptrace.NewResourceSpans(),
		)
		require.NoError(t, err)
		assert.True(t, shouldDrop, "Root span should be dropped when DropRootSpans is true and condition matches")
	})
}
