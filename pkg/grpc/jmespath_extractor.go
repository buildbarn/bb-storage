package grpc

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/auth"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
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
//		"files": map<string, string>,
//	    "incomingGRPCMetadata": map<string, repeated string>
//	}
func NewJMESPathMetadataExtractor(expression *jmespath.JMESPath, files *MetadataFileProvider) (MetadataExtractor, error) {
	return func(ctx context.Context) (MetadataHeaderValues, error) {
		searchContext := make(map[string]interface{}, 3)
		if authenticationMetadata := auth.AuthenticationMetadataFromContext(ctx); authenticationMetadata != nil {
			searchContext["authenticationMetadata"] = authenticationMetadata.GetRaw()
		}

		if files != nil {
			searchContext["files"] = files.getMetadata()
		}

		if md, ok := metadata.FromIncomingContext(ctx); ok {
			// JMESPath only treats map[string]interface{}, struct, or *struct as map types,
			// so we need to copy from the map[string][]string.
			incomingGRPCMetadata := make(map[string]any, len(md))
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

// MetadataFileProvider makes the contents of files available as
// metadata to JMESPath expressions.
type MetadataFileProvider struct {
	lock            sync.RWMutex
	files           []*pb.ClientConfiguration_RefreshedFile
	currentContents map[string]string
}

// NewJMESPathMetadataFileProvider creates a MetadataFileProvider that
// reads files from the filesystem and makes their contents available
// as metadata. The contents of the files are reloaded periodically.
func NewJMESPathMetadataFileProvider(ctx context.Context, files []*pb.ClientConfiguration_RefreshedFile) (*MetadataFileProvider, error) {
	provider := &MetadataFileProvider{
		files:           files,
		currentContents: make(map[string]string),
	}
	for _, file := range files {
		contents, err := readFile(file.Path)
		if err != nil {
			return nil, err
		}
		provider.currentContents[file.Key] = contents
		go func() {
			t := time.NewTicker(file.RefreshInterval.AsDuration())
			for {
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					return
				}
				contents, err := readFile(file.Path)
				if err != nil {
					log.Printf("Failed to reload %s file: %v", file.Path, err)
				} else {
					provider.lock.Lock()
					provider.currentContents[file.Key] = contents
					provider.lock.Unlock()
				}
			}
		}()
	}

	return provider, nil
}

func readFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", util.StatusWrapf(err, "Failed to read %q", path)
	}
	return string(content), nil
}

func (p *MetadataFileProvider) getMetadata() map[string]any {
	p.lock.RLock()
	defer p.lock.RUnlock()
	metadata := make(map[string]any, len(p.currentContents))
	for k, v := range p.currentContents {
		metadata[k] = v
	}
	return metadata
}
