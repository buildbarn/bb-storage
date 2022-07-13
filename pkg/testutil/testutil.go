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
			t.Fatalf("Not equal: want: %#v, got: %#v", wantStr, gotStr)
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

type protoMatches struct {
	t     *testing.T
	proto proto.Message
}

// EqProto is a gomock matcher for proto equality.
func EqProto(t *testing.T, proto proto.Message) gomock.Matcher {
	return &protoMatches{t, proto}
}

func (p *protoMatches) Matches(other interface{}) bool {
	otherProto, ok := other.(proto.Message)
	if ok {
		return proto.Equal(p.proto, otherProto)
	}
	return false
}

func (p *protoMatches) String() string {
	return "is proto equal to " + mustMarshalToString(p.t, p.proto)
}

type statusMatches struct {
	status error
}

// EqPrefixedStatus is a gomock matcher for grps Status equality allowing
// trailing characters in the message.
func EqPrefixedStatus(status error) gomock.Matcher {
	return &statusMatches{status}
}

func (s *statusMatches) Matches(got interface{}) bool {
	gotError, ok := got.(error)
	if !ok {
		return false
	}
	gotProto := status.Convert(gotError).Proto()
	matchProto := status.Convert(s.status).Proto()
	if !strings.HasPrefix(gotProto.GetMessage(), matchProto.GetMessage()) {
		return false
	}
	originalMessage := matchProto.GetMessage()
	matchProto.Message = gotProto.GetMessage()
	eq := proto.Equal(gotProto, matchProto)
	matchProto.Message = originalMessage
	return eq
}

func (s *statusMatches) String() string {
	return fmt.Sprintf("is status equal to %v", s.status)
}

func mustMarshalToString(t *testing.T, proto proto.Message) string {
	s, err := protojson.Marshal(proto)
	if err != nil {
		t.Fatal(err)
	}
	return string(s)
}
