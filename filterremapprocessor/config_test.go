package filterremapprocessor

import (
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            5 * time.Minute,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
				DropRootSpans:                false,
				FlushOnShutdown:              true,
			},
			expectError: false,
		},
		{
			name: "zero max trace retention",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            0,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
			},
			expectError: true,
			errorMsg:    "trace_retention must be greater than 0",
		},
		{
			name: "negative max trace retention",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            -1 * time.Minute,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
			},
			expectError: true,
			errorMsg:    "trace_retention must be greater than 0",
		},
		{
			name: "max trace retention exceeds 10 minutes",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            15 * time.Minute,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
			},
			expectError: true,
			errorMsg:    "trace_retention should not exceed 10 minutes",
		},
		{
			name: "zero last span timeout",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            5 * time.Minute,
				LastSpanTimeout:              0,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
			},
			expectError: true,
			errorMsg:    "last_span_timeout must be greater than 0",
		},
		{
			name: "negative last span timeout",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            5 * time.Minute,
				LastSpanTimeout:              -10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
			},
			expectError: true,
			errorMsg:    "last_span_timeout must be greater than 0",
		},
		{
			name: "zero num traces",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            5 * time.Minute,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    0,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
			},
			expectError: true,
			errorMsg:    "num_traces must be greater than 0",
		},
		{
			name: "zero expected average spans per trace",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            5 * time.Minute,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 0,
			},
			expectError: true,
			errorMsg:    "expected_average_spans_per_trace must be greater than 0",
		},
		{
			name: "valid config with span conditions",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            5 * time.Minute,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
				Traces: TraceFilters{
					SpanConditions: []string{
						`attributes["http.status_code"] == 500`,
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid span condition",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            5 * time.Minute,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
				Traces: TraceFilters{
					SpanConditions: []string{
						`invalid ottl expression {{`,
					},
				},
			},
			expectError: true,
		},
		{
			name: "valid config with span event conditions",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            5 * time.Minute,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
				Traces: TraceFilters{
					SpanEventConditions: []string{
						`name == "exception"`,
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid span event condition",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            5 * time.Minute,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
				Traces: TraceFilters{
					SpanEventConditions: []string{
						`invalid ottl expression {{`,
					},
				},
			},
			expectError: true,
		},
		{
			name: "edge case - exactly 10 minutes retention",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            10 * time.Minute,
				LastSpanTimeout:              10 * time.Second,
				NumTraces:                    10000,
				ExpectedNewTracesPerSec:      100,
				ExpectedAverageSpansPerTrace: 50,
			},
			expectError: false,
		},
		{
			name: "minimal valid config",
			config: Config{
				ErrorMode:                    ottl.PropagateError,
				MaxTraceRetention:            1 * time.Second,
				LastSpanTimeout:              1 * time.Second,
				NumTraces:                    1,
				ExpectedNewTracesPerSec:      1,
				ExpectedAverageSpansPerTrace: 1,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := &Config{
		MaxTraceRetention:            5 * time.Minute,
		LastSpanTimeout:              10 * time.Second,
		NumTraces:                    10000,
		ExpectedNewTracesPerSec:      100,
		ExpectedAverageSpansPerTrace: 50,
	}

	// Test default values
	assert.False(t, cfg.DropRootSpans, "DropRootSpans should default to false")
	assert.False(t, cfg.FlushOnShutdown, "FlushOnShutdown should default to false")
	assert.Equal(t, ottl.ErrorMode(""), cfg.ErrorMode, "ErrorMode should be empty string by default")
}

func TestTraceFiltersEmpty(t *testing.T) {
	// Test that empty trace filters are valid
	cfg := &Config{
		ErrorMode:                    ottl.PropagateError,
		MaxTraceRetention:            5 * time.Minute,
		LastSpanTimeout:              10 * time.Second,
		NumTraces:                    10000,
		ExpectedNewTracesPerSec:      100,
		ExpectedAverageSpansPerTrace: 50,
		Traces:                       TraceFilters{},
	}

	err := cfg.Validate()
	require.NoError(t, err, "Empty trace filters should be valid")
}
