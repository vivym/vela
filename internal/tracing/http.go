package tracing

import (
	"net/http"
	"strconv"

	"github.com/felixge/httpsnoop"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// HTTPServer belongs inside the router middleware stack so the resolved route
// pattern is available. It never records URLs, query strings, headers or bodies.
// httpsnoop preserves optional interfaces needed by streaming responses.
func HTTPServer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagation.TraceContext{}.Extract(r.Context(), parentCarrier{propagation.HeaderCarrier(r.Header)})
		method := "OTHER"
		switch r.Method {
		case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS":
			method = r.Method
		}
		ctx, span := otel.Tracer("vela/http").Start(ctx, method+" unmatched", trace.WithSpanKind(trace.SpanKindServer))
		defer func() {
			if value := recover(); value != nil {
				span.SetStatus(codes.Error, "handler panic")
				// SDK Span.End records an active panic if called directly by
				// defer. End before rethrowing so its message cannot leak.
				span.End()
				panic(value)
			}
			span.End()
		}()
		r = r.WithContext(ctx)
		observed := httpsnoop.CaptureMetrics(next, w, r)
		route := "unmatched"
		if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
			route = rc.RoutePattern()
		}
		span.SetName(method + " " + route)
		span.SetAttributes(attribute.String("http.request.method", method),
			attribute.String("http.route", route), attribute.Int("http.response.status_code", observed.Code))
		if observed.Code >= 500 {
			span.SetStatus(codes.Error, strconv.Itoa(observed.Code))
		}
	})
}
