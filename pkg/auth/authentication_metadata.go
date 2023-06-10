package auth

import (
	"context"
	"encoding/json"

	"github.com/buildbarn/bb-storage/pkg/otel"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/attribute"
)

// AuthenticationMetadata contains information on the authentication
// user that is performing the current operation.
type AuthenticationMetadata struct {
	raw               map[string]any
	proto             auth_pb.AuthenticationMetadata
	tracingAttributes []attribute.KeyValue
}

// NewAuthenticationMetadataFromProto creates a new
// AuthenticationMetadata object that contains the data obtained by the
// gRPC Authenticator.
func NewAuthenticationMetadataFromProto(message *auth_pb.AuthenticationMetadata) (*AuthenticationMetadata, error) {
	// Create raw value, which stores the contents of the Protobuf
	// message as an object normally returned by json.Unmarshal().
	// This can be used to do JMESPath matching by the authorization
	// layer.
	messageJSON, err := protojson.Marshal(message)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Cannot convert authentication metadata to JSON")
	}
	var raw map[string]any
	if err := json.Unmarshal(messageJSON, &raw); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Cannot parse authentication metadata JSON")
	}

	tracingAttributes, err := otel.NewKeyValueListFromProto(message.GetTracingAttributes(), "auth.")
	if err != nil {
		return nil, util.StatusWrap(err, "Cannot create tracing attributes")
	}

	am := &AuthenticationMetadata{
		raw:               raw,
		tracingAttributes: tracingAttributes,
	}
	proto.Merge(&am.proto, message)
	return am, nil
}

// NewAuthenticationMetadataFromRaw is identical to
// NewAuthenticationMetadataFromProto, except that it takes the metadata
// as a JSON-like value (i.e., a map[string]any).
func NewAuthenticationMetadataFromRaw(metadataRaw any) (*AuthenticationMetadata, error) {
	metadataJSON, err := json.Marshal(metadataRaw)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to convert raw authentication metadata to JSON")
	}
	var metadataMessage auth_pb.AuthenticationMetadata
	if err := protojson.Unmarshal(metadataJSON, &metadataMessage); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to convert JSON authentication metadata to Protobuf message")
	}
	return NewAuthenticationMetadataFromProto(&metadataMessage)
}

// MustNewAuthenticationMetadataFromProto is identical to
// NewAuthenticationMetadataFromProto(), except that it panics upon failure. This
// method is provided for testing.
func MustNewAuthenticationMetadataFromProto(message *auth_pb.AuthenticationMetadata) *AuthenticationMetadata {
	authenticationMetadata, err := NewAuthenticationMetadataFromProto(message)
	if err != nil {
		panic(err)
	}
	return authenticationMetadata
}

// GetRaw returns the original JSON-like value that was used to
// construct the AuthenticationMetadata.
func (am *AuthenticationMetadata) GetRaw() map[string]any {
	return am.raw
}

// GetPublicProto returns the AuthenticationMetadata in Protobuf form,
// only containing the values that are safe to display as part of logs.
//
// This method also returns a boolean value that indicates whether the
// resulting message contains any data to display. When false, it may be
// desirable to suppress displaying it.
func (am *AuthenticationMetadata) GetPublicProto() (*auth_pb.AuthenticationMetadata, bool) {
	return &auth_pb.AuthenticationMetadata{
		Public: am.proto.Public,
	}, am.proto.Public != nil
}

// GetFullProto returns the AuthenticationMetadata in Protobuf form.
func (am *AuthenticationMetadata) GetFullProto() *auth_pb.AuthenticationMetadata {
	return &am.proto
}

// GetTracingAttributes returns OpenTelemetry tracing attributes that
// can be added to spans.
func (am *AuthenticationMetadata) GetTracingAttributes() []attribute.KeyValue {
	return am.tracingAttributes
}

type authenticationMetadataKey struct{}

var defaultAuthenticationMetadata AuthenticationMetadata

// NewContextWithAuthenticationMetadata creates a new Context object
// that has AuthenticationMetadata attached to it.
func NewContextWithAuthenticationMetadata(ctx context.Context, authenticationMetadata *AuthenticationMetadata) context.Context {
	return context.WithValue(ctx, authenticationMetadataKey{}, authenticationMetadata)
}

// AuthenticationMetadataFromContext reobtains the
// AuthenticationMetadata that was attached to the Context object.
//
// If the Context object contains no metadata, a default instance
// corresponding to the empty metadata is returned.
func AuthenticationMetadataFromContext(ctx context.Context) *AuthenticationMetadata {
	if value := ctx.Value(authenticationMetadataKey{}); value != nil {
		return value.(*AuthenticationMetadata)
	}
	return &defaultAuthenticationMetadata
}
