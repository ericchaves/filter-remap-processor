package utils

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspanevent"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
)

func StandardSpanFuncs() map[string]ottl.Factory[ottlspan.TransformContext] {
	// return only the standard converters since we only want to evaluate boolean expressions and not transform data
	return ottlfuncs.StandardConverters[ottlspan.TransformContext]()
}

func StandardSpanEventFuncs() map[string]ottl.Factory[ottlspanevent.TransformContext] {
	// return only the standard converters since we only want to evaluate boolean expressions and not transform data
	return ottlfuncs.StandardConverters[ottlspanevent.TransformContext]()
}
