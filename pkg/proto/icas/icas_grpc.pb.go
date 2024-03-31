// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.23.1
// source: pkg/proto/icas/icas.proto

package icas

import (
	context "context"
	v2 "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	IndirectContentAddressableStorage_FindMissingReferences_FullMethodName = "/buildbarn.icas.IndirectContentAddressableStorage/FindMissingReferences"
	IndirectContentAddressableStorage_BatchUpdateReferences_FullMethodName = "/buildbarn.icas.IndirectContentAddressableStorage/BatchUpdateReferences"
	IndirectContentAddressableStorage_GetReference_FullMethodName          = "/buildbarn.icas.IndirectContentAddressableStorage/GetReference"
)

// IndirectContentAddressableStorageClient is the client API for IndirectContentAddressableStorage service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type IndirectContentAddressableStorageClient interface {
	FindMissingReferences(ctx context.Context, in *v2.FindMissingBlobsRequest, opts ...grpc.CallOption) (*v2.FindMissingBlobsResponse, error)
	BatchUpdateReferences(ctx context.Context, in *BatchUpdateReferencesRequest, opts ...grpc.CallOption) (*v2.BatchUpdateBlobsResponse, error)
	GetReference(ctx context.Context, in *GetReferenceRequest, opts ...grpc.CallOption) (*Reference, error)
}

type indirectContentAddressableStorageClient struct {
	cc grpc.ClientConnInterface
}

func NewIndirectContentAddressableStorageClient(cc grpc.ClientConnInterface) IndirectContentAddressableStorageClient {
	return &indirectContentAddressableStorageClient{cc}
}

func (c *indirectContentAddressableStorageClient) FindMissingReferences(ctx context.Context, in *v2.FindMissingBlobsRequest, opts ...grpc.CallOption) (*v2.FindMissingBlobsResponse, error) {
	out := new(v2.FindMissingBlobsResponse)
	err := c.cc.Invoke(ctx, IndirectContentAddressableStorage_FindMissingReferences_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *indirectContentAddressableStorageClient) BatchUpdateReferences(ctx context.Context, in *BatchUpdateReferencesRequest, opts ...grpc.CallOption) (*v2.BatchUpdateBlobsResponse, error) {
	out := new(v2.BatchUpdateBlobsResponse)
	err := c.cc.Invoke(ctx, IndirectContentAddressableStorage_BatchUpdateReferences_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *indirectContentAddressableStorageClient) GetReference(ctx context.Context, in *GetReferenceRequest, opts ...grpc.CallOption) (*Reference, error) {
	out := new(Reference)
	err := c.cc.Invoke(ctx, IndirectContentAddressableStorage_GetReference_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// IndirectContentAddressableStorageServer is the server API for IndirectContentAddressableStorage service.
// All implementations should embed UnimplementedIndirectContentAddressableStorageServer
// for forward compatibility
type IndirectContentAddressableStorageServer interface {
	FindMissingReferences(context.Context, *v2.FindMissingBlobsRequest) (*v2.FindMissingBlobsResponse, error)
	BatchUpdateReferences(context.Context, *BatchUpdateReferencesRequest) (*v2.BatchUpdateBlobsResponse, error)
	GetReference(context.Context, *GetReferenceRequest) (*Reference, error)
}

// UnimplementedIndirectContentAddressableStorageServer should be embedded to have forward compatible implementations.
type UnimplementedIndirectContentAddressableStorageServer struct {
}

func (UnimplementedIndirectContentAddressableStorageServer) FindMissingReferences(context.Context, *v2.FindMissingBlobsRequest) (*v2.FindMissingBlobsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FindMissingReferences not implemented")
}
func (UnimplementedIndirectContentAddressableStorageServer) BatchUpdateReferences(context.Context, *BatchUpdateReferencesRequest) (*v2.BatchUpdateBlobsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method BatchUpdateReferences not implemented")
}
func (UnimplementedIndirectContentAddressableStorageServer) GetReference(context.Context, *GetReferenceRequest) (*Reference, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetReference not implemented")
}

// UnsafeIndirectContentAddressableStorageServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to IndirectContentAddressableStorageServer will
// result in compilation errors.
type UnsafeIndirectContentAddressableStorageServer interface {
	mustEmbedUnimplementedIndirectContentAddressableStorageServer()
}

func RegisterIndirectContentAddressableStorageServer(s grpc.ServiceRegistrar, srv IndirectContentAddressableStorageServer) {
	s.RegisterService(&IndirectContentAddressableStorage_ServiceDesc, srv)
}

func _IndirectContentAddressableStorage_FindMissingReferences_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(v2.FindMissingBlobsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IndirectContentAddressableStorageServer).FindMissingReferences(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: IndirectContentAddressableStorage_FindMissingReferences_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IndirectContentAddressableStorageServer).FindMissingReferences(ctx, req.(*v2.FindMissingBlobsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IndirectContentAddressableStorage_BatchUpdateReferences_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BatchUpdateReferencesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IndirectContentAddressableStorageServer).BatchUpdateReferences(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: IndirectContentAddressableStorage_BatchUpdateReferences_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IndirectContentAddressableStorageServer).BatchUpdateReferences(ctx, req.(*BatchUpdateReferencesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IndirectContentAddressableStorage_GetReference_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetReferenceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IndirectContentAddressableStorageServer).GetReference(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: IndirectContentAddressableStorage_GetReference_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IndirectContentAddressableStorageServer).GetReference(ctx, req.(*GetReferenceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// IndirectContentAddressableStorage_ServiceDesc is the grpc.ServiceDesc for IndirectContentAddressableStorage service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var IndirectContentAddressableStorage_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "buildbarn.icas.IndirectContentAddressableStorage",
	HandlerType: (*IndirectContentAddressableStorageServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "FindMissingReferences",
			Handler:    _IndirectContentAddressableStorage_FindMissingReferences_Handler,
		},
		{
			MethodName: "BatchUpdateReferences",
			Handler:    _IndirectContentAddressableStorage_BatchUpdateReferences_Handler,
		},
		{
			MethodName: "GetReference",
			Handler:    _IndirectContentAddressableStorage_GetReference_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/proto/icas/icas.proto",
}
