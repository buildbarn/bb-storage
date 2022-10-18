package testutil

import (
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// RequireEqualProto asserts that the two passed protocol buffer
// messages are equal.
//
// Because maps in protocol buffers aren't serialized deterministically
// (and can be embedded into google.protobuf.Any values), this function
// falls back to doing a string comparison upon failure.
func RequireEqualProto(t *testing.T, want, got proto.Message) {
	t.Helper()
	if !proto.Equal(want, got) {
		wantStr := mustMarshalToString(t, want)
		gotStr := mustMarshalToString(t, got)
		if wantStr != gotStr {
			t.Fatalf("Not equal:\nWant:\n\n%s\n\nGot:\n\n%s", wantStr, gotStr)
		}
	}
}

// RequireEqualStatus asserts that two grpc Statuses are equal.
func RequireEqualStatus(t *testing.T, want, got error) {
	t.Helper()
	RequireEqualProto(t, status.Convert(want).Proto(), status.Convert(got).Proto())
}

// RequirePrefixedStatus compares that two errors, assumed to be grpc Statuses,
// are the same, except got may have extra trailing characters in its message.
func RequirePrefixedStatus(t *testing.T, want, got error) {
	wantProto := status.Convert(want).Proto()
	gotProto := status.Convert(got).Proto()
	require.Condition(t, func() bool { return strings.HasPrefix(gotProto.GetMessage(), wantProto.GetMessage()) }, "Want message of status\n%v\nto have prefix\n%v", mustMarshalToString(t, gotProto), wantProto.GetMessage())
	gotProto.Message = wantProto.GetMessage()
	RequireEqualProto(t, wantProto, gotProto)
}

type eqProtoMatcher struct {
	t     *testing.T
	proto proto.Message
}

// EqProto is a gomock matcher for proto equality.
func EqProto(t *testing.T, proto proto.Message) gomock.Matcher {
	return &eqProtoMatcher{
		t:     t,
		proto: proto,
	}
}

func (p *eqProtoMatcher) Matches(other interface{}) bool {
	if otherProto, ok := other.(proto.Message); ok {
		return proto.Equal(p.proto, otherProto)
	}
	return false
}

func (p *eqProtoMatcher) String() string {
	return "is proto equal to " + mustMarshalToString(p.t, p.proto)
}

type eqStatusMatcher struct {
	t             *testing.T
	status        error
	statusMessage proto.Message
}

// EqStatus is a gomock matcher for gRPC status equality.
func EqStatus(t *testing.T, s error) gomock.Matcher {
	return &eqStatusMatcher{
		t:             t,
		status:        s,
		statusMessage: status.Convert(s).Proto(),
	}
}

func (s *eqStatusMatcher) Matches(got interface{}) bool {
	if gotError, ok := got.(error); ok {
		gotMessage := status.Convert(gotError).Proto()
		return proto.Equal(s.statusMessage, gotMessage) || mustMarshalToString(s.t, s.statusMessage) == mustMarshalToString(s.t, gotMessage)
	}
	return false
}

func (s *eqStatusMatcher) String() string {
	return fmt.Sprintf("is status equal to %v", s.status)
}

type eqPrefixedStatusMatcher struct {
	status error
}

// EqPrefixedStatus is a gomock matcher for gRPC status equality
// allowing trailing characters in the message.
func EqPrefixedStatus(status error) gomock.Matcher {
	return &eqPrefixedStatusMatcher{
		status: status,
	}
}

func (s *eqPrefixedStatusMatcher) Matches(got interface{}) bool {
	if gotError, ok := got.(error); ok {
		gotProto := status.Convert(gotError).Proto()
		matchProto := status.Convert(s.status).Proto()
		if strings.HasPrefix(gotProto.GetMessage(), matchProto.GetMessage()) {
			originalMessage := matchProto.GetMessage()
			matchProto.Message = gotProto.GetMessage()
			eq := proto.Equal(gotProto, matchProto)
			matchProto.Message = originalMessage
			return eq
		}
	}
	return false
}

func (s *eqPrefixedStatusMatcher) String() string {
	return fmt.Sprintf("is status equal to %v", s.status)
}

func mustMarshalToString(t *testing.T, proto proto.Message) string {
	s, err := protojson.MarshalOptions{
		Multiline: true,
	}.Marshal(proto)
	if err != nil {
		t.Fatal(err)
	}
	return string(s)
}
