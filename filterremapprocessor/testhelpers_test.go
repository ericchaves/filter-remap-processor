package filterremapprocessor

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// mockTracesConsumer is a mock implementation of consumer.Traces for testing
type mockTracesConsumer struct {
	mu         sync.Mutex
	traces     []ptrace.Traces
	consumeErr error
}

func newMockTracesConsumer() *mockTracesConsumer {
	return &mockTracesConsumer{
		traces: make([]ptrace.Traces, 0),
	}
}

func (m *mockTracesConsumer) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.consumeErr != nil {
		return m.consumeErr
	}

	// Make a copy to avoid data races
	traceCopy := ptrace.NewTraces()
	td.CopyTo(traceCopy)
	m.traces = append(m.traces, traceCopy)
	return nil
}

func (m *mockTracesConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (m *mockTracesConsumer) getTraces() []ptrace.Traces {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.traces
}

func (m *mockTracesConsumer) getTraceCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.traces)
}

func (m *mockTracesConsumer) getTotalSpanCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	totalSpans := 0
	for _, trace := range m.traces {
		for i := 0; i < trace.ResourceSpans().Len(); i++ {
			rs := trace.ResourceSpans().At(i)
			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				ss := rs.ScopeSpans().At(j)
				totalSpans += ss.Spans().Len()
			}
		}
	}
	return totalSpans
}

func (m *mockTracesConsumer) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.traces = make([]ptrace.Traces, 0)
}

// Test helper functions for creating test data

// createTestSpan creates a span with the given IDs and name
func createTestSpan(spanID, parentSpanID pcommon.SpanID, traceID pcommon.TraceID, name string) ptrace.Span {
	span := ptrace.NewSpan()
	span.SetSpanID(spanID)
	span.SetParentSpanID(parentSpanID)
	span.SetTraceID(traceID)
	span.SetName(name)
	span.SetKind(ptrace.SpanKindServer)
	return span
}

// createTestTraces creates a ptrace.Traces object with the given spans
func createTestTraces(spans []ptrace.Span) ptrace.Traces {
	traces := ptrace.NewTraces()

	if len(spans) == 0 {
		return traces
	}

	rs := traces.ResourceSpans().AppendEmpty()
	resource := rs.Resource()
	resource.Attributes().PutStr("service.name", "test-service")

	ss := rs.ScopeSpans().AppendEmpty()
	scope := ss.Scope()
	scope.SetName("test-scope")

	for _, span := range spans {
		destSpan := ss.Spans().AppendEmpty()
		span.CopyTo(destSpan)
	}

	return traces
}

// createSimpleTrace creates a simple trace with 3 spans: root -> child1 -> child2
func createSimpleTrace(traceID pcommon.TraceID) ptrace.Traces {
	rootSpanID := newSpanID(1)
	child1SpanID := newSpanID(2)
	child2SpanID := newSpanID(3)

	rootSpan := createTestSpan(rootSpanID, pcommon.SpanID{}, traceID, "root")
	child1Span := createTestSpan(child1SpanID, rootSpanID, traceID, "child1")
	child2Span := createTestSpan(child2SpanID, child1SpanID, traceID, "child2")

	return createTestTraces([]ptrace.Span{rootSpan, child1Span, child2Span})
}

// createComplexTrace creates a more complex trace hierarchy
func createComplexTrace(traceID pcommon.TraceID) ptrace.Traces {
	/*
		Hierarchy:
		root (1)
		  ├─ child1 (2)
		  │   ├─ grandchild1 (3)
		  │   └─ grandchild2 (4)
		  └─ child2 (5)
		      └─ grandchild3 (6)
	*/

	spans := []ptrace.Span{
		createTestSpan(newSpanID(1), pcommon.SpanID{}, traceID, "root"),
		createTestSpan(newSpanID(2), newSpanID(1), traceID, "child1"),
		createTestSpan(newSpanID(3), newSpanID(2), traceID, "grandchild1"),
		createTestSpan(newSpanID(4), newSpanID(2), traceID, "grandchild2"),
		createTestSpan(newSpanID(5), newSpanID(1), traceID, "child2"),
		createTestSpan(newSpanID(6), newSpanID(5), traceID, "grandchild3"),
	}

	return createTestTraces(spans)
}

// addAttributesToSpan adds attributes to a span
func addAttributesToSpan(span ptrace.Span, attributes map[string]interface{}) {
	for key, value := range attributes {
		switch v := value.(type) {
		case string:
			span.Attributes().PutStr(key, v)
		case int:
			span.Attributes().PutInt(key, int64(v))
		case int64:
			span.Attributes().PutInt(key, v)
		case bool:
			span.Attributes().PutBool(key, v)
		case float64:
			span.Attributes().PutDouble(key, v)
		}
	}
}

// getSpansFromTraces extracts all spans from a traces object
func getSpansFromTraces(traces ptrace.Traces) []ptrace.Span {
	spans := make([]ptrace.Span, 0)

	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rs := traces.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				spans = append(spans, ss.Spans().At(k))
			}
		}
	}

	return spans
}

// findSpanByID finds a span by its span ID in a traces object
func findSpanByID(traces ptrace.Traces, spanID pcommon.SpanID) *ptrace.Span {
	spans := getSpansFromTraces(traces)
	for i := range spans {
		if spans[i].SpanID() == spanID {
			return &spans[i]
		}
	}
	return nil
}

// getSpanNames extracts all span names from a traces object
func getSpanNames(traces ptrace.Traces) []string {
	names := make([]string, 0)
	spans := getSpansFromTraces(traces)
	for _, span := range spans {
		names = append(names, span.Name())
	}
	return names
}
