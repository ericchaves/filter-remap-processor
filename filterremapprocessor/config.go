package filterremapprocessor

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/luke-moehlenbrock/otel-collector-filter-remap-processor/filterremapprocessor/internal/utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	// "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	// "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspanevent"
)

// Config is the configuration for the filter remap processor. (filterprocessor says it's the configuration for Resource processor? Need to figure out what this means...)
type Config struct {
	ErrorMode ottl.ErrorMode `mapstructure:"error_mode"`

	// The amount of time a trace is retained in the processor before it's spans are remapped and sent to the next consumer, regardless of when the last span was seen (in seconds)
	MaxTraceRetention time.Duration `mapstructure:"max_trace_retention"`

	// Time to wait after the last span is seen before remapping and forwarding the trace (in seconds)
	LastSpanTimeout time.Duration `mapstructure:"last_span_timeout"`

	Traces TraceFilters `mapstructure:"traces"`

	// NumTraces is the number of traces kept on memory. Typically most of the data
	// of a trace is released after a sampling decision is taken.
	NumTraces uint64 `mapstructure:"num_traces"`
	// ExpectedNewTracesPerSec sets the expected number of new traces sending to the tail sampling processor
	// per second. This helps with allocating data structures with closer to actual usage size.
	ExpectedNewTracesPerSec uint64 `mapstructure:"expected_new_traces_per_sec"`
	// ExpectedAverageSpansPerTrace is used to allocate data structures for processing traces
	ExpectedAverageSpansPerTrace uint64 `mapstructure:"expected_average_spans_per_trace"`
	// If a root span matches the filter, it will be dropped. Defaults to false
	DropRootSpans bool `mapstructure:"drop_root_spans"`
	// If a span comes in after the last span timeout, we've already forwarded the trace to the next consumer and do not have the spans parent to know whether the parent was dropped or retained.
	// By default, we will just forward any orphaned spans without remapping them, but if this is set to true, we will remap the orphaned spans to root spans.
	RemapOrphanedSpans bool `mapstructure:"remap_orphaned_spans"`
	FlushOnShutdown    bool `mapstructure:"flush_on_shutdown"`
}

type TraceFilters struct {
	SpanConditions []string `mapstructure:"span"`

	SpanEventConditions []string `mapstructure:"spanevent"`
}

var _ component.Config = (*Config)(nil)

func (cfg *Config) Validate() error {

	var errs error

	if cfg.Traces.SpanConditions != nil {
		_, err := utils.NewBoolExprForSpan(cfg.Traces.SpanConditions, utils.StandardSpanFuncs(), ottl.PropagateError, component.TelemetrySettings{Logger: zap.NewNop()})
		errs = multierr.Append(errs, err)
	}

	// TODO: Do we care about span events?
	if cfg.Traces.SpanEventConditions != nil {
		_, err := utils.NewBoolExprForSpanEvent(cfg.Traces.SpanEventConditions, utils.StandardSpanEventFuncs(), ottl.PropagateError, component.TelemetrySettings{Logger: zap.NewNop()})
		errs = multierr.Append(errs, err)
	}

	if cfg.MaxTraceRetention <= 0 {
		err := errors.New("trace_retention must be greater than 0")
		errs = multierr.Append(errs, err)
	}

	if cfg.MaxTraceRetention > 10*time.Minute {
		err := errors.New("trace_retention should not exceed 10 minutes")
		errs = multierr.Append(errs, err)
	}

	if cfg.LastSpanTimeout <= 0 {
		err := errors.New("last_span_timeout must be greater than 0")
		errs = multierr.Append(errs, err)
	}

	if cfg.ExpectedAverageSpansPerTrace < 1 {
		err := errors.New("expected_average_spans_per_trace must be greater than 0")
		errs = multierr.Append(errs, err)
	}

	if cfg.NumTraces < 1 {
		err := errors.New("num_traces must be greater than 0")
		errs = multierr.Append(errs, err)
	}

	return errs
}
