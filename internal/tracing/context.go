package tracing

import "go.opentelemetry.io/otel/propagation"

// Only the validated W3C traceparent crosses application boundaries. Baggage and
// vendor tracestate can contain arbitrary caller data, so neither is accepted
// into exported spans or propagated to another service.
type parentCarrier struct{ propagation.TextMapCarrier }

func (c parentCarrier) Get(key string) string {
	if key == "traceparent" {
		return c.TextMapCarrier.Get(key)
	}
	return ""
}

func (c parentCarrier) Set(key, value string) {
	if key == "traceparent" {
		c.TextMapCarrier.Set(key, value)
	}
}

func (c parentCarrier) Keys() []string { return []string{"traceparent"} }
