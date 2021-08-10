package otel_test

import (
	"context"
	_ "embed" // For "go:embed".
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/otel"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

//go:embed bootstrap.css
var bootstrapCSS string

func requireEqualBody(t *testing.T, expectedBody string, hh http.Handler) {
	//  Perform a HTTP request.
	rec := httptest.NewRecorder()
	hh.ServeHTTP(rec, nil)
	body := rec.Result().Body
	data, _ := io.ReadAll(body)
	body.Close()

	// Compare the response body, ignoring whitespace.
	fullBody := `
		<!DOCTYPE html>
		<html>
			<head>
				<title>Active OpenTelemetry spans</title>
				<style>` + bootstrapCSS + `</style>
			</head>
			<body>
				<nav class="navbar navbar-dark bg-primary">
					<div class="container-fluid">
						<span class="navbar-brand">Active OpenTelemetry spans</span>
					</div>
				</nav>
				<div class="mx-3"> ` + expectedBody + ` </div>
			</body>
		</html>`
	require.Equal(
		t,
		strings.Join(strings.Fields(fullBody), "\n"),
		strings.Join(strings.Fields(string(data)), "\n"))
}

func TestActiveSpansReportingHTTPHandler(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	clock := mock.NewMockClock(ctrl)
	hh := otel.NewActiveSpansReportingHTTPHandler(clock)

	t.Run("EmptyPage", func(t *testing.T) {
		// The page should be empty by default.
		requireEqualBody(t, "", hh)
	})

	baseTracerProvider := mock.NewMockTracerProvider(ctrl)
	tracerProvider := hh.NewTracerProvider(baseTracerProvider)

	baseTracer := mock.NewMockTracer(ctrl)
	baseTracerProvider.EXPECT().Tracer("go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc").Return(baseTracer)
	tracer := tracerProvider.Tracer("go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc")

	t.Run("SingleSpan", func(t *testing.T) {
		// Create a single span.
		baseSpan := mock.NewMockSpan(ctrl)
		baseSpan.EXPECT().SpanContext().Return(trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: [...]byte{0x9c, 0xb1, 0x7a, 0xc2, 0xc1, 0x15, 0x2b, 0x09, 0x6f, 0xfc, 0xb0, 0x38, 0x62, 0x30, 0x4d, 0xeb},
			SpanID:  [...]byte{0x6c, 0xa2, 0x0d, 0x65, 0xb5, 0x0f, 0xce, 0xf5},
			Remote:  true,
		})).AnyTimes()
		baseSpanCtx := context.Background()
		baseTracer.EXPECT().Start(
			ctx,
			"buildbarn.remoteworker.OperationQueue/Synchronize",
			trace.WithAttributes(attribute.String("string", "hello")),
		).Return(baseSpanCtx, baseSpan)
		clock.EXPECT().Now().Return(time.Unix(1628497200, 0))
		_, span := tracer.Start(
			ctx,
			"buildbarn.remoteworker.OperationQueue/Synchronize",
			trace.WithAttributes(attribute.String("string", "hello")))

		requireEqualBody(t, `
			<div class="card my-3">
				<h5 class="bg-dark card-header text-white">Span "buildbarn.remoteworker.OperationQueue/Synchronize"</h5>
				<div class="card-body">
					<table class="table table-sm">
						<thead>
							<tr>
								<th>Start timestamp</th>
								<td>2021-08-09T08:20:00Z</td>
							</tr>
							<tr>
								<th>Instrumentation name</th>
								<td>go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc</td>
							</tr>
							<tr>
								<th>Trace ID</th>
								<td>9cb17ac2c1152b096ffcb03862304deb</td>
							</tr>
							<tr>
								<th>Span ID</th>
								<td>6ca20d65b50fcef5</td>
							</tr>
							<tr>
								<th>Is remote</th>
								<td>true</td>
							</tr>
							<tr>
								<th>Is sampled</th>
								<td>false</td>
							</tr>
						</thead>
						<tbody>
							<tr>
								<th>string</th>
								<td>hello</td>
							</tr>
						</tbody>
					</table>
				</div>
			</div>
		`, hh)

		// Set various properties on the span, and check that all of
		// those are rendered properly.
		baseSpan.EXPECT().AddEvent(
			"SomeEvent",
			trace.WithAttributes(
				attribute.Array("array", []int64{1, 2, 3, 4}),
				attribute.Bool("bool", true)),
			trace.WithTimestamp(time.Unix(1628497200, 100000000)))
		span.AddEvent(
			"SomeEvent",
			trace.WithAttributes(
				attribute.Array("array", []int64{1, 2, 3, 4}),
				attribute.Bool("bool", true)),
			trace.WithTimestamp(time.Unix(1628497200, 100000000)))

		baseSpan.EXPECT().RecordError(
			errors.New("Some error message"),
			trace.WithAttributes(
				attribute.Float64("float64", 1.5),
				attribute.Int("int", 42)),
			trace.WithTimestamp(time.Unix(1628497200, 200000000)))
		span.RecordError(
			errors.New("Some error message"),
			trace.WithAttributes(
				attribute.Float64("float64", 1.5),
				attribute.Int("int", 42)),
			trace.WithTimestamp(time.Unix(1628497200, 200000000)))

		baseSpan.EXPECT().SetStatus(codes.Error, "Some status message")
		span.SetStatus(codes.Error, "Some status message")

		baseSpan.EXPECT().SetName("SomeOtherName")
		span.SetName("SomeOtherName")

		baseSpan.EXPECT().SetAttributes(
			attribute.Int64("int64", 123),
			attribute.String("string", "world"))
		span.SetAttributes(
			attribute.Int64("int64", 123),
			attribute.String("string", "world"))

		requireEqualBody(t, `
			<div class="card my-3">
				<h5 class="bg-dark card-header text-white">Span "SomeOtherName"</h5>
				<div class="card-body">
					<table class="table table-sm">
						<thead>
							<tr>
								<th>Start timestamp</th>
								<td>2021-08-09T08:20:00Z</td>
							</tr>
							<tr>
								<th>Instrumentation name</th>
								<td>go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc</td>
							</tr>
							<tr>
								<th>Status</th>
								<td>
									<span class="badge bg-danger">Error</span>
									Some status message
								</td>
							</tr>
							<tr>
								<th>Trace ID</th>
								<td>9cb17ac2c1152b096ffcb03862304deb</td>
							</tr>
							<tr>
								<th>Span ID</th>
								<td>6ca20d65b50fcef5</td>
							</tr>
							<tr>
								<th>Is remote</th>
								<td>true</td>
							</tr>
							<tr>
								<th>Is sampled</th>
								<td>false</td>
							</tr>
						</thead>
						<tbody>
							<tr>
								<th>int64</th>
								<td>123</td>
							</tr>
							<tr>
								<th>string</th>
								<td>world</td>
							</tr>
						</tbody>
					</table>
					<div class="card my-3">
						<h5 class="bg-primary card-header text-white">Last event "SomeEvent"</h5>
						<div class="card-body">
							<table class="table table-sm">
								<thead>
									<tr>
										<th>Timestamp</th>
										<td>2021-08-09T08:20:00.1Z</td>
									</tr>
								</thead>
								<tbody>
									<tr>
										<th>array</th>
										<td>[1 2 3 4]</td>
									</tr>
									<tr>
										<th>bool</th>
										<td>true</td>
									</tr>
								</tbody>
							</table>
						</div>
					</div>
					<div class="card my-3">
						<h5 class="bg-danger card-header text-white">Last error "Some error message"</h5>
						<div class="card-body">
							<table class="table table-sm">
								<thead>
									<tr>
										<th>Timestamp</th>
										<td>2021-08-09T08:20:00.2Z</td>
									</tr>
								</thead>
								<tbody>
									<tr>
										<th>float64</th>
										<td>1.5</td>
									</tr>
									<tr>
										<th>int</th>
										<td>42</td>
									</tr>
								</tbody>
							</table>
						</div>
					</div>
				</div>
			</div>
		`, hh)

		// End the span. It should be removed from the page.
		clock.EXPECT().Now().Return(time.Unix(1628497201, 0))
		baseSpan.EXPECT().End()
		span.End()

		requireEqualBody(t, "", hh)
	})

	t.Run("NestedSpans", func(t *testing.T) {
		// Create two nested spans. End the parent span before
		// the child. The parent span should continue to be
		// shown, with the end timestamp set.
		baseSpan1 := mock.NewMockSpan(ctrl)
		baseSpan1.EXPECT().SpanContext().Return(trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: [...]byte{0xd4, 0x67, 0x2a, 0xfe, 0xb1, 0x6b, 0x2d, 0x24, 0x05, 0xf2, 0x55, 0x30, 0xa0, 0xf3, 0x36, 0x31},
			SpanID:  [...]byte{0xb2, 0x0c, 0x23, 0xe5, 0x5a, 0x11, 0xd1, 0x1d},
		})).AnyTimes()
		baseSpan1Ctx := context.Background()
		baseTracer.EXPECT().Start(ctx, "span1").Return(baseSpan1Ctx, baseSpan1)
		clock.EXPECT().Now().Return(time.Unix(1628596553, 0))
		span1Ctx, span1 := tracer.Start(ctx, "span1")

		baseSpan2 := mock.NewMockSpan(ctrl)
		baseSpan2.EXPECT().SpanContext().Return(trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: [...]byte{0xd4, 0x67, 0x2a, 0xfe, 0xb1, 0x6b, 0x2d, 0x24, 0x05, 0xf2, 0x55, 0x30, 0xa0, 0xf3, 0x36, 0x31},
			SpanID:  [...]byte{0x01, 0x39, 0xde, 0x2d, 0x12, 0xd0, 0x0b, 0xe2},
		})).AnyTimes()
		baseSpan2Ctx := context.Background()
		baseTracer.EXPECT().Start(span1Ctx, "span2").Return(baseSpan2Ctx, baseSpan2)
		clock.EXPECT().Now().Return(time.Unix(1628596554, 0))
		_, span2 := tracer.Start(span1Ctx, "span2")

		clock.EXPECT().Now().Return(time.Unix(1628596555, 0))
		baseSpan1.EXPECT().End()
		span1.End()

		requireEqualBody(t, `
			<div class="card my-3">
				<h5 class="bg-dark card-header text-white">Span "span1"</h5>
				<div class="card-body">
					<table class="table table-sm">
						<thead>
							<tr>
								<th>Start timestamp</th>
								<td>2021-08-10T11:55:53Z</td>
							</tr>
							<tr>
								<th>End timestamp</th>
								<td>2021-08-10T11:55:55Z</td>
							</tr>
							<tr>
								<th>Instrumentation name</th>
								<td>go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc</td>
							</tr>
							<tr>
								<th>Trace ID</th>
								<td>d4672afeb16b2d2405f25530a0f33631</td>
							</tr>
							<tr>
								<th>Span ID</th>
								<td>b20c23e55a11d11d</td>
							</tr>
							<tr>
								<th>Is remote</th>
								<td>false</td>
							</tr>
							<tr>
								<th>Is sampled</th>
								<td>false</td>
							</tr>
						</thead>
					</table>
					<div class="card my-3">
						<h5 class="bg-dark card-header text-white">Span "span2"</h5>
						<div class="card-body">
							<table class="table table-sm">
								<thead>
									<tr>
										<th>Start timestamp</th>
										<td>2021-08-10T11:55:54Z</td>
									</tr>
									<tr>
										<th>Instrumentation name</th>
										<td>go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc</td>
									</tr>
									<tr>
										<th>Trace ID</th>
										<td>d4672afeb16b2d2405f25530a0f33631</td>
									</tr>
									<tr>
										<th>Span ID</th>
										<td>0139de2d12d00be2</td>
									</tr>
									<tr>
										<th>Is remote</th>
										<td>false</td>
									</tr>
									<tr>
										<th>Is sampled</th>
										<td>false</td>
									</tr>
								</thead>
							</table>
						</div>
					</div>
				</div>
			</div>
		`, hh)

		// Ending the child span should cause the parent to be
		// removed as well, as it was already ended and doesn't
		// have any other children.
		clock.EXPECT().Now().Return(time.Unix(1628596556, 0))
		baseSpan2.EXPECT().End()
		span2.End()

		requireEqualBody(t, "", hh)
	})
}
