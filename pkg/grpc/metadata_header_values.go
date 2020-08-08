package grpc

// MetadataHeaderValues is a utility type for generating pairs of header
// names and values that need to be provided to
// metadata.AppendToOutgoingContext().
type MetadataHeaderValues []string

// Add one or more values for a given header name.
func (hv *MetadataHeaderValues) Add(header string, values []string) {
	for _, value := range values {
		*hv = append(*hv, header, value)
	}
}
