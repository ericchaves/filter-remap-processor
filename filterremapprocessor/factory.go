//go:generate mdatagen metadata.yaml

package filterremapprocessor

import (
	"context"
	"time"

	"github.com/luke-moehlenbrock/otel-collector-filter-remap-processor/filterremapprocessor/internal/metadata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
	)
}

// Note: Need to keep in mind that the values we don't set a default for will be set to 0, so explicitly set values where that is not okay and add checks elsewhere
func createDefaultConfig() component.Config {
	return &Config{
		ErrorMode:                    ottl.PropagateError,
		MaxTraceRetention:            10 * time.Minute,
		LastSpanTimeout:              10 * time.Second,
		NumTraces:                    10000,
		ExpectedNewTracesPerSec:      15,
		ExpectedAverageSpansPerTrace: 20,
		DropRootSpans:                false,
		RemapOrphanedSpans:           false,
		FlushOnShutdown:              false,
	}
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Traces,
) (processor.Traces, error) {
	return newFilterRemapProcessor(ctx, set, next, cfg.(*Config))
}
