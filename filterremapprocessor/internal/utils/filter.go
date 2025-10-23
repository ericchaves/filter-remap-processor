// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package utils // import "github.com/luke-moehlenbrock/otel-collector-filter-remap-processor/filterremapprocessor/internal/utils"

import (
	"go.opentelemetry.io/collector/component"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspanevent"
)

func NewBoolExprForSpan(conditions []string, functions map[string]ottl.Factory[ottlspan.TransformContext], errorMode ottl.ErrorMode, settings component.TelemetrySettings) (*ottl.ConditionSequence[ottlspan.TransformContext], error) {
	parser, err := ottlspan.NewParser(functions, settings)
	if err != nil {
		return nil, err
	}
	statements, err := parser.ParseConditions(conditions)
	if err != nil {
		return nil, err
	}
	c := ottlspan.NewConditionSequence(statements, settings, ottlspan.WithConditionSequenceErrorMode(errorMode))
	return &c, nil
}

func NewBoolExprForSpanEvent(conditions []string, functions map[string]ottl.Factory[ottlspanevent.TransformContext], errorMode ottl.ErrorMode, settings component.TelemetrySettings) (*ottl.ConditionSequence[ottlspanevent.TransformContext], error) {
	parser, err := ottlspanevent.NewParser(functions, settings)
	if err != nil {
		return nil, err
	}
	statements, err := parser.ParseConditions(conditions)
	if err != nil {
		return nil, err
	}
	c := ottlspanevent.NewConditionSequence(statements, settings, ottlspanevent.WithConditionSequenceErrorMode(errorMode))
	return &c, nil
}
