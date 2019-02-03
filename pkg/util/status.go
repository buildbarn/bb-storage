package util

import (
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
