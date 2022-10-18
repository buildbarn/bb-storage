package otel_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/otel"
	"github.com/stretchr/testify/require"
)

func TestW3CTraceContext(t *testing.T) {
	t.Run("NoW3CTraceContextInContext", func(t *testing.T) {
		// In contexts where this method is called without any
		// W3C Trace Context being present in the current
		// Context, it should just return an empty map.
		ctx := context.Background()
		require.Empty(t, otel.W3CTraceContextFromContext(ctx))
	})

	t.Run("NonStandardHeaders", func(t *testing.T) {
		// Headers that are not part of the W3C Trace Context
		// specification should simply be ignored.
		ctx := otel.NewContextWithW3CTraceContext(
			context.Background(),
			map[string]string{
				"hello": "world",
			})
		require.Empty(t, otel.W3CTraceContextFromContext(ctx))
	})

	t.Run("GarbageTraceparent", func(t *testing.T) {
		// The traceparent header has a strictly defined
		// structure according to the W3C Trace Context
		// specification. Invalid values should be discarded.
		// More details:
		// https://www.w3.org/TR/trace-context/#version-format
		ctx := otel.NewContextWithW3CTraceContext(
			context.Background(),
			map[string]string{
				"traceparent": "This is a garbage value",
			})
		require.Empty(t, otel.W3CTraceContextFromContext(ctx))
	})

	t.Run("ValidTraceparent", func(t *testing.T) {
		// If the traceparent header is valid, its value should
		// be forwarded when the Context is converted to a W3C
		// Trace Context once again.
		ctx := otel.NewContextWithW3CTraceContext(
			context.Background(),
			map[string]string{
				"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			})
		require.Equal(
			t,
			map[string]string{
				"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			},
			otel.W3CTraceContextFromContext(ctx))
	})
}
