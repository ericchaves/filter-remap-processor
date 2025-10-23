package filterremapprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Test helper to create span IDs
func newSpanID(id byte) pcommon.SpanID {
	spanID := pcommon.SpanID([8]byte{0, 0, 0, 0, 0, 0, 0, id})
	return spanID
}

// Test helper to create trace IDs
func newTraceID(id byte) pcommon.TraceID {
	traceID := pcommon.TraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, id})
	return traceID
}

func TestRetainedHierarchyNode(t *testing.T) {
	t.Run("basic properties", func(t *testing.T) {
		span := ptrace.NewSpan()
		spanID := newSpanID(1)
		parentID := newSpanID(2)
		span.SetSpanID(spanID)
		span.SetParentSpanID(parentID)

		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		node := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &span,
				instrumentationScope: &scope,
				resource:             &resource,
				scopeSchemaUrl:       "schema-url",
				resourceSchemaUrl:    "resource-schema-url",
			},
			parentRetained: true,
		}

		assert.Equal(t, spanID, node.SpanID())
		assert.Equal(t, parentID, node.ParentID())
		assert.True(t, node.IsParentRetained())
		assert.NotNil(t, node.SpanData())
	})

	t.Run("set parent id", func(t *testing.T) {
		span := ptrace.NewSpan()
		span.SetSpanID(newSpanID(1))
		span.SetParentSpanID(newSpanID(2))

		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		node := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &span,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: false,
		}

		newParentID := newSpanID(3)
		node.setParentId(newParentID, true)

		assert.Equal(t, newParentID, node.ParentID())
		assert.True(t, node.IsParentRetained())
		// Verify the underlying span was also updated
		assert.Equal(t, newParentID, span.ParentSpanID())
	})

	t.Run("empty parent id", func(t *testing.T) {
		span := ptrace.NewSpan()
		span.SetSpanID(newSpanID(1))

		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		node := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &span,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: false,
		}

		emptyParentID := pcommon.SpanID{}
		node.setParentId(emptyParentID, true)

		assert.True(t, node.ParentID().IsEmpty())
		assert.True(t, node.IsParentRetained())
	})
}

func TestDroppedHierarchyNode(t *testing.T) {
	t.Run("basic properties", func(t *testing.T) {
		spanID := newSpanID(1)
		traceID := newTraceID(1)
		parentID := newSpanID(2)

		node := &droppedHierarchyNode{
			spanId:         spanID,
			traceId:        traceID,
			parentSpanId:   parentID,
			parentRetained: true,
		}

		assert.Equal(t, spanID, node.SpanID())
		assert.Equal(t, parentID, node.ParentID())
		assert.True(t, node.IsParentRetained())
	})

	t.Run("set parent id", func(t *testing.T) {
		node := &droppedHierarchyNode{
			spanId:         newSpanID(1),
			traceId:        newTraceID(1),
			parentSpanId:   newSpanID(2),
			parentRetained: false,
		}

		newParentID := newSpanID(3)
		node.setParentId(newParentID, true)

		assert.Equal(t, newParentID, node.ParentID())
		assert.True(t, node.IsParentRetained())
	})

	t.Run("empty parent id", func(t *testing.T) {
		node := &droppedHierarchyNode{
			spanId:         newSpanID(1),
			traceId:        newTraceID(1),
			parentSpanId:   newSpanID(2),
			parentRetained: false,
		}

		emptyParentID := pcommon.SpanID{}
		node.setParentId(emptyParentID, true)

		assert.True(t, node.ParentID().IsEmpty())
		assert.True(t, node.IsParentRetained())
	})

	t.Run("parent not retained", func(t *testing.T) {
		node := &droppedHierarchyNode{
			spanId:         newSpanID(1),
			traceId:        newTraceID(1),
			parentSpanId:   newSpanID(2),
			parentRetained: false,
		}

		assert.False(t, node.IsParentRetained())

		node.setParentId(newSpanID(3), false)
		assert.False(t, node.IsParentRetained())
	})
}

func TestSpanAndScope(t *testing.T) {
	t.Run("preserves all fields", func(t *testing.T) {
		span := ptrace.NewSpan()
		span.SetName("test-span")
		span.SetSpanID(newSpanID(1))

		resource := pcommon.NewResource()
		resource.Attributes().PutStr("service.name", "test-service")

		scope := pcommon.NewInstrumentationScope()
		scope.SetName("test-scope")

		scopeSchema := "scope-schema-v1"
		resourceSchema := "resource-schema-v1"

		spanAndScope := &spanAndScope{
			span:                 &span,
			instrumentationScope: &scope,
			resource:             &resource,
			scopeSchemaUrl:       scopeSchema,
			resourceSchemaUrl:    resourceSchema,
		}

		assert.Equal(t, "test-span", spanAndScope.span.Name())
		assert.Equal(t, "test-service", spanAndScope.resource.Attributes().AsRaw()["service.name"])
		assert.Equal(t, "test-scope", spanAndScope.instrumentationScope.Name())
		assert.Equal(t, scopeSchema, spanAndScope.scopeSchemaUrl)
		assert.Equal(t, resourceSchema, spanAndScope.resourceSchemaUrl)
	})

	t.Run("pointer preservation", func(t *testing.T) {
		// Test that pointers are preserved correctly
		span := ptrace.NewSpan()
		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		ss1 := &spanAndScope{
			span:                 &span,
			instrumentationScope: &scope,
			resource:             &resource,
		}

		ss2 := &spanAndScope{
			span:                 &span,
			instrumentationScope: &scope,
			resource:             &resource,
		}

		// Both should point to the same underlying objects
		assert.Same(t, ss1.span, ss2.span)
		assert.Same(t, ss1.instrumentationScope, ss2.instrumentationScope)
		assert.Same(t, ss1.resource, ss2.resource)
	})
}

func TestHierarchyNodeInterface(t *testing.T) {
	t.Run("retained node implements interface", func(t *testing.T) {
		span := ptrace.NewSpan()
		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		var node hierarchyNode = &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &span,
				instrumentationScope: &scope,
				resource:             &resource,
			},
		}

		assert.NotNil(t, node)
		assert.Implements(t, (*hierarchyNode)(nil), node)
	})

	t.Run("dropped node implements interface", func(t *testing.T) {
		var node hierarchyNode = &droppedHierarchyNode{
			spanId:       newSpanID(1),
			traceId:      newTraceID(1),
			parentSpanId: newSpanID(2),
		}

		assert.NotNil(t, node)
		assert.Implements(t, (*hierarchyNode)(nil), node)
	})

	t.Run("retained node implements retainedNode interface", func(t *testing.T) {
		span := ptrace.NewSpan()
		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		var node retainedNode = &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &span,
				instrumentationScope: &scope,
				resource:             &resource,
			},
		}

		assert.NotNil(t, node)
		assert.NotNil(t, node.SpanData())
	})
}
