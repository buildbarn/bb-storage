package auth

import (
	"context"
	"encoding/json"

	"github.com/buildbarn/bb-storage/pkg/otel"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	"go.opentelemetry.io/otel/attribute"
)

// AuthenticationMetadata contains information on the authentication
// user that is performing the current operation.
type AuthenticationMetadata struct {
	raw               any
	proto             auth_pb.AuthenticationMetadata
	tracingAttributes []attribute.KeyValue
}

// NewAuthenticationMetadata creates a new AuthenticationMetadata object
// that contains the data obtained by the gRPC Authenticator.
func NewAuthenticationMetadata(raw any) (*AuthenticationMetadata, error) {
	// If the authentication metadata is object-like, attempt to
	// extract predefined fields from it as well.
	//
	// TODO: Should we require that metadata is object-like?
	var public *structpb.Value
	var tracingAttributes []attribute.KeyValue
	if _, ok := raw.(map[string]any); ok {
		rawJSON, err := json.Marshal(raw)
		if err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Cannot convert authentication metadata to JSON")
		}
		var message auth_pb.AuthenticationMetadata
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(rawJSON, &message); err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Cannot convert authentication metadata to Protobuf")
		}
		public = message.Public

		tracingAttributes, err = otel.NewKeyValueListFromProto(message.TracingAttributes, "auth.")
		if err != nil {
			return nil, util.StatusWrap(err, "Cannot create tracing attributes")
		}
	}

	return &AuthenticationMetadata{
		raw: raw,
		proto: auth_pb.AuthenticationMetadata{
			Public: public,
		},
		tracingAttributes: tracingAttributes,
	}, nil
}

// MustNewAuthenticationMetadata is identical to NewAuthenticationMetadata(),
// except that it panics upon failure. This method is provided for
// testing.
func MustNewAuthenticationMetadata(raw any) *AuthenticationMetadata {
	authenticationMetadata, err := NewAuthenticationMetadata(raw)
	if err != nil {
		panic(err)
	}
	return authenticationMetadata
}

// GetRaw returns the original JSON-like value that was used to
// construct the AuthenticationMetadata.
func (am *AuthenticationMetadata) GetRaw() any {
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
