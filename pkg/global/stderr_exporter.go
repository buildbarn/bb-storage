package global

import (
	"fmt"
	"log"
	"strings"
	"time"

	"go.opencensus.io/trace"
	"google.golang.org/grpc/codes"
)

type stderrExporter struct{}

// NewStderrExporter produces the trivial wiring to route trace spans
// to stderr. This is noisy and only intended for basic debugging.
func NewStderrExporter() trace.Exporter {
	return stderrExporter{}
}

func (stderrExporter) ExportSpan(sd *trace.SpanData) {
	log.Printf("%s %s %s %s %s %s\n",
		sd.StartTime.Format(time.RFC3339),
		sd.EndTime.Sub(sd.StartTime).String(),
		sd.Name,
		codes.Code(sd.Status.Code).String(),
		sd.Status.Message,
		formatAnnotations(sd.Annotations),
	)
}

func formatAnnotations(annotations []trace.Annotation) string {
	var out strings.Builder
	for _, annotation := range annotations {
		out.WriteString(annotation.Message)
		out.WriteString("{")
		first := true
		for key, value := range annotation.Attributes {
			if !first {
				out.WriteString(",")
				first = false
			}
			out.WriteString(key)
			out.WriteString("=")
			out.WriteString(fmt.Sprintf("%#v", value))
		}
		out.WriteString("{")
	}
	return out.String()
}
