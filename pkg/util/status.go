package util

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StatusWrap prepends a string to the message of an existing error.
func StatusWrap(err error, msg string) error {
	p := status.Convert(err).Proto()
	p.Message = fmt.Sprintf("%s: %s", msg, p.Message)
	return status.ErrorProto(p)
}

// StatusWrapf prepends a formatted string to the message of an existing error.
func StatusWrapf(err error, format string, args ...interface{}) error {
	return StatusWrap(err, fmt.Sprintf(format, args...))
}

// StatusWrapWithCode prepends a string to the message of an existing
// error, while replacing the error code.
func StatusWrapWithCode(err error, code codes.Code, msg string) error {
	p := status.Convert(err).Proto()
	p.Code = int32(code)
	p.Message = fmt.Sprintf("%s: %s", msg, p.Message)
	return status.ErrorProto(p)
}

// StatusWrapfWithCode prepends a formatted string to the message of an
// existing error, while replacing the error code.
func StatusWrapfWithCode(err error, code codes.Code, format string, args ...interface{}) error {
	return StatusWrapWithCode(err, code, fmt.Sprintf(format, args...))
}

// StatusFromContext converts the error associated with a context to a
// gRPC Status error. This function ensures that errors such as
// context.DeadlineExceeded are properly converted to Status objects
// having the code DEADLINE_EXCEEDED.
func StatusFromContext(ctx context.Context) error {
	if s := status.FromContextError(ctx.Err()); s != nil {
		return s.Err()
	}
	return nil
}

// IsInfrastructureError returns true if an error is caused by a failure
// of the infrastructure, as opposed to it being caused by a parameter
// provided by the caller.
//
// This function may, for example, be used to determine whether a call
// should be retried.
func IsInfrastructureError(err error) bool {
	code := status.Code(err)
	return code == codes.Internal || code == codes.Unavailable || code == codes.Unknown
}
