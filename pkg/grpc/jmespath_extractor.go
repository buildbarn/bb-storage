package grpc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// NewJMESPathMetadataExtractor creates a MetadataExtractor by evaluating a
// JMESPath expression.
// The expression is expected to return a map[string][]string with all keys
// lower-case, and will populate a header per key.
//
// The JMESPath expression is called against a JSON object with the following
// structure:
//
//	{
//	    "authenticationMetadata": value,
//	    "incomingGRPCMetadata": map<string, repeated string>
//	}
func NewJMESPathMetadataExtractor(expression *jmespath.JMESPath) (MetadataExtractor, error) {
	return func(ctx context.Context) (MetadataHeaderValues, error) {
		searchContext := make(map[string]interface{}, 2)
		if authenticationMetadata := auth.AuthenticationMetadataFromContext(ctx); authenticationMetadata != nil {
			searchContext["authenticationMetadata"] = authenticationMetadata.GetRaw()
		}

		if md, ok := metadata.FromIncomingContext(ctx); ok {
			// JMESPath only treats map[string]interface{}, struct, or *struct as map types,
			// so we need to copy from the map[string][]string.
			incomingGRPCMetadata := make(map[string]interface{}, len(md))
			for k, rawVs := range md {
				vs := make([]interface{}, 0, len(rawVs))
				for _, rawV := range rawVs {
					vs = append(vs, rawV)
				}
				incomingGRPCMetadata[k] = vs
			}
			searchContext["incomingGRPCMetadata"] = incomingGRPCMetadata
		}

		rawMatch, err := expression.Search(searchContext)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to evaluate JMESPath")
		}
		if rawMatch == nil {
			return nil, status.Error(codes.NotFound, "No match evaluating JMESPath")
		}
		headers, err := matchToHeaders(rawMatch)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to extract JMESPath result")
		}
		return headers, nil
	}, nil
}

func matchToHeaders(rawMatch interface{}) (MetadataHeaderValues, error) {
	match, ok := rawMatch.(map[string]interface{})
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Didn't evaluate to map[string][]string")
	}
	var headers MetadataHeaderValues
	for k, vsRaw := range match {
		if vsRaw == nil {
			continue
		}
		vsSlice, ok := vsRaw.([]interface{})
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Non-slice metadata value")
		}
		for _, vRaw := range vsSlice {
			v, ok := vRaw.(string)
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "Non-string metadata value")
			}
			headers = append(headers, k, v)
		}
	}
	return headers, nil
}
