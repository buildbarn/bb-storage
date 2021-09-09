package http

import (
	"google.golang.org/grpc/codes"
)

// StatusCodeFromGRPCCode returns the HTTP status code that corresponds
// to a gRPC status code. The HTTP status codes returned by this
// function correspond to the values documented in the Protobuf
// defintions of the Code enum:
//
// https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
//
// The implementation of gRPC for Go provides no public method for doing
// this conversion for us.
func StatusCodeFromGRPCCode(code codes.Code) int {
	switch code {
	case codes.Canceled:
		return 499
	case codes.InvalidArgument:
		return 400
	case codes.DeadlineExceeded:
		return 504
	case codes.NotFound:
		return 404
	case codes.AlreadyExists:
		return 409
	case codes.PermissionDenied:
		return 403
	case codes.Unauthenticated:
		return 401
	case codes.ResourceExhausted:
		return 429
	case codes.FailedPrecondition:
		return 400
	case codes.Aborted:
		return 409
	case codes.OutOfRange:
		return 400
	case codes.Unimplemented:
		return 501
	case codes.Unavailable:
		return 503
	default:
		return 500
	}
}
