package otel

import (
	"context"
	_ "embed" // For "go:embed".
	"html/template"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

var (
	//go:embed active_spans.html
	activeSpansTemplateBody string
	activeSpansTemplate     = template.Must(template.New("ActiveSpans").Funcs(template.FuncMap{
		"stylesheet": func() template.CSS { return stylesheet },
		"timestamp_rfc3339": func(t time.Time) string {
			// Converts a timestamp to RFC3339 format.
			return t.Format("2006-01-02T15:04:05.999Z07:00")
		},
	}).Parse(activeSpansTemplateBody))
	//go:embed stylesheet.css
	stylesheet template.CSS
)

// ActiveSpansReportingHTTPHandler is a HTTP handler that can generate a
// single page that lists all of the OpenTelemetry spans that have not
// been ended, or contain one or more transitive children that have not
// been ended.
type ActiveSpansReportingHTTPHandler struct {
	clock clock.Clock

	lock  sync.Mutex
	spans spanList
}

var _ http.Handler = &ActiveSpansReportingHTTPHandler{}

// NewActiveSpansReportingHTTPHandler creates a HTTP handler that can
// generate a single page that lists all active OpenTelemetry spans.
func NewActiveSpansReportingHTTPHandler(clock clock.Clock) *ActiveSpansReportingHTTPHandler {
	return &ActiveSpansReportingHTTPHandler{
		clock: clock,
	}
}

// NewTracerProvider decorates an OpenTelemetry TracerProvider in such a
// way that all spans created through it are shown on the web page
// emitted by ServeHTTP().
func (hh *ActiveSpansReportingHTTPHandler) NewTracerProvider(base trace.TracerProvider) trace.TracerProvider {
	return &activeSpansReportingTracerProvider{
		base:        base,
		httpHandler: hh,
	}
}

// Data model of information displayed through the template.
type spanInfo struct {
	storedSpanInfo

	InstrumentationName string
	SpanContext         trace.SpanContext
	Attributes          map[attribute.Key]attribute.Value
	Children            []spanInfo
}

type storedSpanInfo struct {
	StartTimestamp time.Time
	Name           string
	EndTimestamp   time.Time
	LastEvent      *eventInfo
	LastError      *errorInfo
	Status         *statusInfo
}

type eventInfo struct {
	Name        string
	EventConfig trace.EventConfig
}

type errorInfo struct {
	Err         error
	EventConfig trace.EventConfig
}

type statusInfo struct {
	IsError     bool
	Description string
}

func (hh *ActiveSpansReportingHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hh.lock.Lock()
	spans := hh.spans.getInfos()
	hh.lock.Unlock()
	if err := activeSpansTemplate.Execute(w, spans); err != nil {
		log.Print("Failed to report active spans: ", err)
	}
}

// ActiveSpansReportingTracerProvider is a decorator for TracerProvider
// that causes all resulting Spans to be wrapped.
type activeSpansReportingTracerProvider struct {
	embedded.TracerProvider

	base        trace.TracerProvider
	httpHandler *ActiveSpansReportingHTTPHandler
}

func (tp *activeSpansReportingTracerProvider) Tracer(instrumentationName string, opts ...trace.TracerOption) trace.Tracer {
	return &activeSpansReportingTracer{
		base:                tp.base.Tracer(instrumentationName, opts...),
		tracerProvider:      tp,
		instrumentationName: instrumentationName,
	}
}

// ActiveSpansReportingTracer is a decorator for Tracer that causes all
// resulting Spans to be wrapped.
type activeSpansReportingTracer struct {
	embedded.Tracer

	base                trace.Tracer
	tracerProvider      *activeSpansReportingTracerProvider
	instrumentationName string
}

func (t *activeSpansReportingTracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	baseCtx, baseSpan := t.base.Start(ctx, spanName, opts...)

	// Extract attributes and start timestamp from the options.
	hh := t.tracerProvider.httpHandler
	startConfig := trace.NewSpanStartConfig(opts...)
	keyValues := startConfig.Attributes()
	attributes := make(map[attribute.Key]attribute.Value, len(keyValues))
	for _, keyValue := range keyValues {
		attributes[keyValue.Key] = keyValue.Value
	}
	startTimestamp := startConfig.Timestamp()
	if startTimestamp.IsZero() {
		startTimestamp = hh.clock.Now()
	}

	span := &activeSpan{
		base:       baseSpan,
		tracer:     t,
		attributes: attributes,
		info: storedSpanInfo{
			StartTimestamp: startTimestamp,
			Name:           spanName,
		},
	}

	// Insert span into the bookkeeping.
	hh.lock.Lock()
	spanList := &hh.spans
	parentSpan, ok := trace.SpanFromContext(ctx).(*activeSpan)
	if ok && parentSpan.tracer.tracerProvider.httpHandler == hh && parentSpan.info.EndTimestamp.IsZero() {
		// Parent belongs to this HTTP handler and hasn't been
		// ended yet. Place child inside.
		spanList = &parentSpan.children
		span.parent = parentSpan
	} else {
		// Span has no parent, or the parent has already been
		// ended. Attach this span directly to the root.
		parentSpan = nil
	}
	span.older = spanList.newest
	if spanList.oldest == nil {
		spanList.oldest = span
	} else {
		spanList.newest.newer = span
	}
	spanList.newest = span
	hh.lock.Unlock()

	return trace.ContextWithSpan(baseCtx, span), span
}

// SpanList is a doubly linked list of spans that all belong to the same
// parent. Spans are sorted in the list by creation time.
type spanList struct {
	oldest *activeSpan
	newest *activeSpan
}

func (l *spanList) remove(s *activeSpan) {
	if l.oldest == s {
		l.oldest = s.newer
	} else {
		s.older.newer = s.newer
	}
	if l.newest == s {
		l.newest = s.older
	} else {
		s.newer.older = s.older
	}
	s.older = nil
	s.newer = nil
}

func (l *spanList) getInfos() []spanInfo {
	var infos []spanInfo
	for s := l.oldest; s != nil; s = s.newer {
		// We can't copy the attributes map directly, as it may
		// get mutated after the lock gets released.
		attributes := make(map[attribute.Key]attribute.Value, len(s.attributes))
		for key, value := range s.attributes {
			attributes[key] = value
		}
		infos = append(infos, spanInfo{
			storedSpanInfo:      s.info,
			InstrumentationName: s.tracer.instrumentationName,
			SpanContext:         s.base.SpanContext(),
			Attributes:          attributes,
			Children:            s.children.getInfos(),
		})
	}
	return infos
}

// ActiveSpan contains all of the bookkeeping for a single span
// displayed by ActiveSpansReportingHTTPHandler.
type activeSpan struct {
	embedded.Span

	// Constant fields.
	base   trace.Span
	tracer *activeSpansReportingTracer
	parent *activeSpan

	// Fields that are cleared upon removal.
	older *activeSpan
	newer *activeSpan

	// Fields that are mutated through the Span interface.
	attributes map[attribute.Key]attribute.Value
	children   spanList
	info       storedSpanInfo
}

func (s *activeSpan) End(options ...trace.SpanEndOption) {
	// NewSpanEndConfig() can only set the timestamp field inside
	// SpanConfig, as only WithTimestamp() implements the
	// SpanEndOption interface. Just extract that field, instead of
	// storing the entire SpanConfig.
	hh := s.tracer.tracerProvider.httpHandler
	spanEndConfig := trace.NewSpanEndConfig(options...)
	endTimestamp := spanEndConfig.Timestamp()
	if endTimestamp.IsZero() {
		endTimestamp = hh.clock.Now()
	}

	hh.lock.Lock()
	if s.info.EndTimestamp.IsZero() {
		s.info.EndTimestamp = endTimestamp

		// Garbage collect the span, and any parents that have
		// ended as well, and no longer have any children.
		for sRemove := s; sRemove.children.oldest == nil; {
			if sParent := sRemove.parent; sParent == nil {
				// Reached the root.
				hh.spans.remove(sRemove)
				break
			} else {
				sParent.children.remove(sRemove)
				if sParent.info.EndTimestamp.IsZero() {
					// Parent has not ended yet.
					break
				}
				sRemove = sParent
			}
		}
	}
	hh.lock.Unlock()

	s.base.End(options...)
}

func (s *activeSpan) AddEvent(name string, options ...trace.EventOption) {
	lastEvent := &eventInfo{
		Name:        name,
		EventConfig: trace.NewEventConfig(options...),
	}

	hh := s.tracer.tracerProvider.httpHandler
	hh.lock.Lock()
	s.info.LastEvent = lastEvent
	hh.lock.Unlock()

	s.base.AddEvent(name, options...)
}

func (s *activeSpan) AddLink(link trace.Link) {
	s.base.AddLink(link)
}

func (activeSpan) IsRecording() bool {
	// Return true, even though the underlying span may not be
	// recording. This ensures that the creator of the span
	// continues to provide events.
	return true
}

func (s *activeSpan) RecordError(err error, options ...trace.EventOption) {
	lastError := &errorInfo{
		Err:         err,
		EventConfig: trace.NewEventConfig(options...),
	}

	hh := s.tracer.tracerProvider.httpHandler
	hh.lock.Lock()
	s.info.LastError = lastError
	hh.lock.Unlock()

	s.base.RecordError(err, options...)
}

func (s *activeSpan) SpanContext() trace.SpanContext {
	return s.base.SpanContext()
}

func (s *activeSpan) SetStatus(code codes.Code, description string) {
	hh := s.tracer.tracerProvider.httpHandler
	hh.lock.Lock()
	switch code {
	case codes.Unset:
		s.info.Status = nil
	case codes.Error:
		s.info.Status = &statusInfo{
			IsError:     true,
			Description: description,
		}
	case codes.Ok:
		s.info.Status = &statusInfo{
			IsError:     false,
			Description: description,
		}
	default:
		panic("Unknown status code")
	}
	hh.lock.Unlock()

	s.base.SetStatus(code, description)
}

func (s *activeSpan) SetName(name string) {
	hh := s.tracer.tracerProvider.httpHandler
	hh.lock.Lock()
	s.info.Name = name
	hh.lock.Unlock()

	s.base.SetName(name)
}

func (s *activeSpan) SetAttributes(keyValues ...attribute.KeyValue) {
	hh := s.tracer.tracerProvider.httpHandler
	hh.lock.Lock()
	for _, keyValue := range keyValues {
		s.attributes[keyValue.Key] = keyValue.Value
	}
	hh.lock.Unlock()

	s.base.SetAttributes(keyValues...)
}

func (s *activeSpan) TracerProvider() trace.TracerProvider {
	return s.tracer.tracerProvider
}
