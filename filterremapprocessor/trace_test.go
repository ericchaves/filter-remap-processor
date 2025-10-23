package filterremapprocessor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestHierarchyMapBasicOperations(t *testing.T) {
	t.Run("get and set", func(t *testing.T) {
		hm := hierarchyMap{
			m: make(map[pcommon.SpanID]hierarchyNode),
		}

		spanID := newSpanID(1)
		node := &droppedHierarchyNode{
			spanId:  spanID,
			traceId: newTraceID(1),
		}

		// Test set
		hm.set(spanID, node)

		// Test get
		retrieved, ok := hm.get(spanID)
		assert.True(t, ok)
		assert.Equal(t, node, retrieved)
	})

	t.Run("get non-existent", func(t *testing.T) {
		hm := hierarchyMap{
			m: make(map[pcommon.SpanID]hierarchyNode),
		}

		retrieved, ok := hm.get(newSpanID(99))
		assert.False(t, ok)
		assert.Nil(t, retrieved)
	})

	t.Run("concurrent access", func(t *testing.T) {
		hm := hierarchyMap{
			m: make(map[pcommon.SpanID]hierarchyNode),
		}

		// Spawn multiple goroutines to test concurrent access
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func(id byte) {
				spanID := newSpanID(id)
				node := &droppedHierarchyNode{
					spanId:  spanID,
					traceId: newTraceID(1),
				}
				hm.set(spanID, node)

				retrieved, ok := hm.get(spanID)
				assert.True(t, ok)
				assert.Equal(t, spanID, retrieved.SpanID())
				done <- true
			}(byte(i))
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestHierarchyMapRemapping(t *testing.T) {
	t.Run("simple parent-child with dropped parent", func(t *testing.T) {
		hm := hierarchyMap{
			m: make(map[pcommon.SpanID]hierarchyNode),
		}

		// Create hierarchy: grandparent (retained) -> parent (dropped) -> child (retained)
		grandparentSpan := ptrace.NewSpan()
		grandparentID := newSpanID(1)
		grandparentSpan.SetSpanID(grandparentID)
		grandparentSpan.SetParentSpanID(pcommon.SpanID{}) // root

		parentID := newSpanID(2)

		childSpan := ptrace.NewSpan()
		childID := newSpanID(3)
		childSpan.SetSpanID(childID)
		childSpan.SetParentSpanID(parentID)

		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		grandparentNode := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &grandparentSpan,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: true, // root span
		}

		parentNode := &droppedHierarchyNode{
			spanId:         parentID,
			traceId:        newTraceID(1),
			parentSpanId:   grandparentID,
			parentRetained: false,
		}

		childNode := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &childSpan,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: false,
		}

		hm.set(grandparentID, grandparentNode)
		hm.set(parentID, parentNode)
		hm.set(childID, childNode)

		// Remap the hierarchy
		hm.remapHierarchy(true)

		// Child should now point to grandparent
		assert.Equal(t, grandparentID, childSpan.ParentSpanID())
		assert.True(t, childNode.IsParentRetained())
		assert.True(t, parentNode.IsParentRetained())
	})

	t.Run("multiple dropped parents in chain", func(t *testing.T) {
		hm := hierarchyMap{
			m: make(map[pcommon.SpanID]hierarchyNode),
		}

		// Create hierarchy: root (retained) -> drop1 -> drop2 -> drop3 -> child (retained)
		rootSpan := ptrace.NewSpan()
		rootID := newSpanID(1)
		rootSpan.SetSpanID(rootID)
		rootSpan.SetParentSpanID(pcommon.SpanID{})

		drop1ID := newSpanID(2)
		drop2ID := newSpanID(3)
		drop3ID := newSpanID(4)

		childSpan := ptrace.NewSpan()
		childID := newSpanID(5)
		childSpan.SetSpanID(childID)
		childSpan.SetParentSpanID(drop3ID)

		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		rootNode := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &rootSpan,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: true,
		}

		drop1Node := &droppedHierarchyNode{
			spanId:         drop1ID,
			traceId:        newTraceID(1),
			parentSpanId:   rootID,
			parentRetained: false,
		}

		drop2Node := &droppedHierarchyNode{
			spanId:         drop2ID,
			traceId:        newTraceID(1),
			parentSpanId:   drop1ID,
			parentRetained: false,
		}

		drop3Node := &droppedHierarchyNode{
			spanId:         drop3ID,
			traceId:        newTraceID(1),
			parentSpanId:   drop2ID,
			parentRetained: false,
		}

		childNode := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &childSpan,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: false,
		}

		hm.set(rootID, rootNode)
		hm.set(drop1ID, drop1Node)
		hm.set(drop2ID, drop2Node)
		hm.set(drop3ID, drop3Node)
		hm.set(childID, childNode)

		// Remap the hierarchy
		hm.remapHierarchy(true)

		// Child should now point directly to root
		assert.Equal(t, rootID, childSpan.ParentSpanID())
		assert.True(t, childNode.IsParentRetained())
		// All dropped nodes should also point to root
		assert.Equal(t, rootID, drop3Node.ParentID())
		assert.Equal(t, rootID, drop2Node.ParentID())
		assert.Equal(t, rootID, drop1Node.ParentID())
	})

	t.Run("orphan span with no parent in map - remap to root", func(t *testing.T) {
		hm := hierarchyMap{
			m: make(map[pcommon.SpanID]hierarchyNode),
		}

		// Create a child that references a parent not in the map
		missingParentID := newSpanID(99)

		childSpan := ptrace.NewSpan()
		childID := newSpanID(1)
		childSpan.SetSpanID(childID)
		childSpan.SetParentSpanID(missingParentID)

		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		childNode := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &childSpan,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: false,
		}

		hm.set(childID, childNode)

		// Remap the hierarchy with remapOrphanedSpans=true
		hm.remapHierarchy(true)

		// Child should have empty parent (becomes orphan/root)
		assert.True(t, childSpan.ParentSpanID().IsEmpty())
		assert.False(t, childNode.IsParentRetained())
	})

	t.Run("orphan span with no parent in map - keep original parent", func(t *testing.T) {
		hm := hierarchyMap{
			m: make(map[pcommon.SpanID]hierarchyNode),
		}

		// Create a child that references a parent not in the map
		missingParentID := newSpanID(99)

		childSpan := ptrace.NewSpan()
		childID := newSpanID(1)
		childSpan.SetSpanID(childID)
		childSpan.SetParentSpanID(missingParentID)

		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		childNode := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &childSpan,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: false,
		}

		hm.set(childID, childNode)

		// Remap the hierarchy with remapOrphanedSpans=false
		hm.remapHierarchy(false)

		// Child should keep the original missing parent ID
		assert.Equal(t, missingParentID, childSpan.ParentSpanID())
		assert.False(t, childNode.IsParentRetained())
	})

	t.Run("all retained nodes", func(t *testing.T) {
		hm := hierarchyMap{
			m: make(map[pcommon.SpanID]hierarchyNode),
		}

		// Create hierarchy where all nodes are retained
		parentSpan := ptrace.NewSpan()
		parentID := newSpanID(1)
		parentSpan.SetSpanID(parentID)
		parentSpan.SetParentSpanID(pcommon.SpanID{})

		childSpan := ptrace.NewSpan()
		childID := newSpanID(2)
		childSpan.SetSpanID(childID)
		childSpan.SetParentSpanID(parentID)

		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		parentNode := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &parentSpan,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: true,
		}

		childNode := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &childSpan,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: false,
		}

		hm.set(parentID, parentNode)
		hm.set(childID, childNode)

		// Remap the hierarchy
		hm.remapHierarchy(true)

		// Nothing should change - child still points to parent
		assert.Equal(t, parentID, childSpan.ParentSpanID())
		// After remapping, child should know parent is retained
		assert.True(t, childNode.IsParentRetained())
	})

	t.Run("circular reference protection", func(t *testing.T) {
		hm := hierarchyMap{
			m: make(map[pcommon.SpanID]hierarchyNode),
		}

		// Create a circular reference (should not happen in practice, but we should handle it)
		span1ID := newSpanID(1)
		span2ID := newSpanID(2)

		drop1Node := &droppedHierarchyNode{
			spanId:         span1ID,
			traceId:        newTraceID(1),
			parentSpanId:   span2ID,
			parentRetained: false,
		}

		drop2Node := &droppedHierarchyNode{
			spanId:         span2ID,
			traceId:        newTraceID(1),
			parentSpanId:   span1ID, // circular reference
			parentRetained: false,
		}

		hm.set(span1ID, drop1Node)
		hm.set(span2ID, drop2Node)

		// Remap should not hang or panic
		hm.remapHierarchy(true)

		// Nodes should remain as they were (cycle detection prevents infinite loop)
		assert.False(t, drop1Node.IsParentRetained())
		assert.False(t, drop2Node.IsParentRetained())
	})

	t.Run("dropped root span", func(t *testing.T) {
		hm := hierarchyMap{
			m: make(map[pcommon.SpanID]hierarchyNode),
		}

		// Scenario: root span is dropped, child is retained
		rootID := newSpanID(1)

		childSpan := ptrace.NewSpan()
		childID := newSpanID(2)
		childSpan.SetSpanID(childID)
		childSpan.SetParentSpanID(rootID)

		resource := pcommon.NewResource()
		scope := pcommon.NewInstrumentationScope()

		rootNode := &droppedHierarchyNode{
			spanId:         rootID,
			traceId:        newTraceID(1),
			parentSpanId:   pcommon.SpanID{}, // empty parent (root)
			parentRetained: false,
		}

		childNode := &retainedHierarchyNode{
			spanData: &spanAndScope{
				span:                 &childSpan,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			parentRetained: false,
		}

		hm.set(rootID, rootNode)
		hm.set(childID, childNode)

		// Remap the hierarchy
		hm.remapHierarchy(true)

		// Child should now have empty parent (becomes root)
		assert.True(t, childSpan.ParentSpanID().IsEmpty())
		assert.True(t, childNode.IsParentRetained())
	})
}

func TestTraceData(t *testing.T) {
	t.Run("initialization", func(t *testing.T) {
		now := time.Now()
		td := &traceData{
			ArrivalTime: now,
			HierarchyMap: hierarchyMap{
				m: make(map[pcommon.SpanID]hierarchyNode),
			},
		}

		td.SpanCount.Store(10)
		td.LastSpanArrivalNanos.Store(now.UnixNano())

		assert.Equal(t, now, td.ArrivalTime)
		assert.Equal(t, int64(10), td.SpanCount.Load())
		assert.Equal(t, now.UnixNano(), td.LastSpanArrivalNanos.Load())
	})

	t.Run("span count operations", func(t *testing.T) {
		td := &traceData{
			HierarchyMap: hierarchyMap{
				m: make(map[pcommon.SpanID]hierarchyNode),
			},
		}

		td.SpanCount.Store(5)
		assert.Equal(t, int64(5), td.SpanCount.Load())

		td.SpanCount.Add(3)
		assert.Equal(t, int64(8), td.SpanCount.Load())

		td.SpanCount.Add(-2)
		assert.Equal(t, int64(6), td.SpanCount.Load())
	})

	t.Run("last span arrival tracking", func(t *testing.T) {
		td := &traceData{
			HierarchyMap: hierarchyMap{
				m: make(map[pcommon.SpanID]hierarchyNode),
			},
		}

		time1 := time.Now()
		td.LastSpanArrivalNanos.Store(time1.UnixNano())

		time.Sleep(1 * time.Millisecond)

		time2 := time.Now()
		td.LastSpanArrivalNanos.Store(time2.UnixNano())

		assert.True(t, td.LastSpanArrivalNanos.Load() > time1.UnixNano())
		assert.Equal(t, time2.UnixNano(), td.LastSpanArrivalNanos.Load())
	})
}

func TestBuildRemappedTrace(t *testing.T) {
	t.Run("single span", func(t *testing.T) {
		span := ptrace.NewSpan()
		span.SetName("test-span")
		span.SetSpanID(newSpanID(1))

		resource := pcommon.NewResource()
		resource.Attributes().PutStr("service.name", "test-service")

		scope := pcommon.NewInstrumentationScope()
		scope.SetName("test-scope")

		spanAndScopes := []spanAndScope{
			{
				span:                 &span,
				instrumentationScope: &scope,
				resource:             &resource,
				scopeSchemaUrl:       "scope-schema",
				resourceSchemaUrl:    "resource-schema",
			},
		}

		traces := buildRemappedTrace(spanAndScopes)

		require.Equal(t, 1, traces.ResourceSpans().Len())
		rs := traces.ResourceSpans().At(0)
		assert.Equal(t, "resource-schema", rs.SchemaUrl())

		require.Equal(t, 1, rs.ScopeSpans().Len())
		ss := rs.ScopeSpans().At(0)
		assert.Equal(t, "scope-schema", ss.SchemaUrl())

		require.Equal(t, 1, ss.Spans().Len())
		resultSpan := ss.Spans().At(0)
		assert.Equal(t, "test-span", resultSpan.Name())
	})

	t.Run("multiple spans same resource and scope", func(t *testing.T) {
		resource := pcommon.NewResource()
		resource.Attributes().PutStr("service.name", "test-service")

		scope := pcommon.NewInstrumentationScope()
		scope.SetName("test-scope")

		span1 := ptrace.NewSpan()
		span1.SetName("span-1")
		span1.SetSpanID(newSpanID(1))

		span2 := ptrace.NewSpan()
		span2.SetName("span-2")
		span2.SetSpanID(newSpanID(2))

		spanAndScopes := []spanAndScope{
			{
				span:                 &span1,
				instrumentationScope: &scope,
				resource:             &resource,
			},
			{
				span:                 &span2,
				instrumentationScope: &scope,
				resource:             &resource,
			},
		}

		traces := buildRemappedTrace(spanAndScopes)

		// Should have 1 resource with 1 scope containing 2 spans
		require.Equal(t, 1, traces.ResourceSpans().Len())
		rs := traces.ResourceSpans().At(0)
		require.Equal(t, 1, rs.ScopeSpans().Len())
		ss := rs.ScopeSpans().At(0)
		assert.Equal(t, 2, ss.Spans().Len())
	})

	t.Run("multiple resources", func(t *testing.T) {
		resource1 := pcommon.NewResource()
		resource1.Attributes().PutStr("service.name", "service-1")

		resource2 := pcommon.NewResource()
		resource2.Attributes().PutStr("service.name", "service-2")

		scope := pcommon.NewInstrumentationScope()

		span1 := ptrace.NewSpan()
		span1.SetSpanID(newSpanID(1))

		span2 := ptrace.NewSpan()
		span2.SetSpanID(newSpanID(2))

		spanAndScopes := []spanAndScope{
			{
				span:                 &span1,
				instrumentationScope: &scope,
				resource:             &resource1,
			},
			{
				span:                 &span2,
				instrumentationScope: &scope,
				resource:             &resource2,
			},
		}

		traces := buildRemappedTrace(spanAndScopes)

		// Should have 2 resources
		assert.Equal(t, 2, traces.ResourceSpans().Len())
	})

	t.Run("empty input", func(t *testing.T) {
		spanAndScopes := []spanAndScope{}
		traces := buildRemappedTrace(spanAndScopes)

		assert.Equal(t, 0, traces.ResourceSpans().Len())
	})
}
