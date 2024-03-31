// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v4.23.1
// source: pkg/proto/icas/icas.proto

package icas

import (
	v2 "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Reference struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Medium:
	//
	//	*Reference_HttpUrl
	//	*Reference_S3_
	//	*Reference_Gcs
	//	*Reference_ContentAddressableStorage_
	Medium       isReference_Medium  `protobuf_oneof:"medium"`
	OffsetBytes  int64               `protobuf:"varint,3,opt,name=offset_bytes,json=offsetBytes,proto3" json:"offset_bytes,omitempty"`
	SizeBytes    int64               `protobuf:"varint,4,opt,name=size_bytes,json=sizeBytes,proto3" json:"size_bytes,omitempty"`
	Decompressor v2.Compressor_Value `protobuf:"varint,6,opt,name=decompressor,proto3,enum=build.bazel.remote.execution.v2.Compressor_Value" json:"decompressor,omitempty"`
}

func (x *Reference) Reset() {
	*x = Reference{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_proto_icas_icas_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Reference) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Reference) ProtoMessage() {}

func (x *Reference) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_icas_icas_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Reference.ProtoReflect.Descriptor instead.
func (*Reference) Descriptor() ([]byte, []int) {
	return file_pkg_proto_icas_icas_proto_rawDescGZIP(), []int{0}
}

func (m *Reference) GetMedium() isReference_Medium {
	if m != nil {
		return m.Medium
	}
	return nil
}

func (x *Reference) GetHttpUrl() string {
	if x, ok := x.GetMedium().(*Reference_HttpUrl); ok {
		return x.HttpUrl
	}
	return ""
}

func (x *Reference) GetS3() *Reference_S3 {
	if x, ok := x.GetMedium().(*Reference_S3_); ok {
		return x.S3
	}
	return nil
}

func (x *Reference) GetGcs() *Reference_GCS {
	if x, ok := x.GetMedium().(*Reference_Gcs); ok {
		return x.Gcs
	}
	return nil
}

func (x *Reference) GetContentAddressableStorage() *Reference_ContentAddressableStorage {
	if x, ok := x.GetMedium().(*Reference_ContentAddressableStorage_); ok {
		return x.ContentAddressableStorage
	}
	return nil
}

func (x *Reference) GetOffsetBytes() int64 {
	if x != nil {
		return x.OffsetBytes
	}
	return 0
}

func (x *Reference) GetSizeBytes() int64 {
	if x != nil {
		return x.SizeBytes
	}
	return 0
}

func (x *Reference) GetDecompressor() v2.Compressor_Value {
	if x != nil {
		return x.Decompressor
	}
	return v2.Compressor_Value(0)
}

type isReference_Medium interface {
	isReference_Medium()
}

type Reference_HttpUrl struct {
	HttpUrl string `protobuf:"bytes,1,opt,name=http_url,json=httpUrl,proto3,oneof"`
}

type Reference_S3_ struct {
	S3 *Reference_S3 `protobuf:"bytes,2,opt,name=s3,proto3,oneof"`
}

type Reference_Gcs struct {
	Gcs *Reference_GCS `protobuf:"bytes,7,opt,name=gcs,proto3,oneof"`
}

type Reference_ContentAddressableStorage_ struct {
	ContentAddressableStorage *Reference_ContentAddressableStorage `protobuf:"bytes,8,opt,name=content_addressable_storage,json=contentAddressableStorage,proto3,oneof"`
}

func (*Reference_HttpUrl) isReference_Medium() {}

func (*Reference_S3_) isReference_Medium() {}

func (*Reference_Gcs) isReference_Medium() {}

func (*Reference_ContentAddressableStorage_) isReference_Medium() {}

type BatchUpdateReferencesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	InstanceName   string                                  `protobuf:"bytes,1,opt,name=instance_name,json=instanceName,proto3" json:"instance_name,omitempty"`
	Requests       []*BatchUpdateReferencesRequest_Request `protobuf:"bytes,2,rep,name=requests,proto3" json:"requests,omitempty"`
	DigestFunction v2.DigestFunction_Value                 `protobuf:"varint,3,opt,name=digest_function,json=digestFunction,proto3,enum=build.bazel.remote.execution.v2.DigestFunction_Value" json:"digest_function,omitempty"`
}

func (x *BatchUpdateReferencesRequest) Reset() {
	*x = BatchUpdateReferencesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_proto_icas_icas_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchUpdateReferencesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchUpdateReferencesRequest) ProtoMessage() {}

func (x *BatchUpdateReferencesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_icas_icas_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchUpdateReferencesRequest.ProtoReflect.Descriptor instead.
func (*BatchUpdateReferencesRequest) Descriptor() ([]byte, []int) {
	return file_pkg_proto_icas_icas_proto_rawDescGZIP(), []int{1}
}

func (x *BatchUpdateReferencesRequest) GetInstanceName() string {
	if x != nil {
		return x.InstanceName
	}
	return ""
}

func (x *BatchUpdateReferencesRequest) GetRequests() []*BatchUpdateReferencesRequest_Request {
	if x != nil {
		return x.Requests
	}
	return nil
}

func (x *BatchUpdateReferencesRequest) GetDigestFunction() v2.DigestFunction_Value {
	if x != nil {
		return x.DigestFunction
	}
	return v2.DigestFunction_Value(0)
}

type GetReferenceRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	InstanceName   string                  `protobuf:"bytes,1,opt,name=instance_name,json=instanceName,proto3" json:"instance_name,omitempty"`
	Digest         *v2.Digest              `protobuf:"bytes,2,opt,name=digest,proto3" json:"digest,omitempty"`
	DigestFunction v2.DigestFunction_Value `protobuf:"varint,3,opt,name=digest_function,json=digestFunction,proto3,enum=build.bazel.remote.execution.v2.DigestFunction_Value" json:"digest_function,omitempty"`
}

func (x *GetReferenceRequest) Reset() {
	*x = GetReferenceRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_proto_icas_icas_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetReferenceRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetReferenceRequest) ProtoMessage() {}

func (x *GetReferenceRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_icas_icas_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetReferenceRequest.ProtoReflect.Descriptor instead.
func (*GetReferenceRequest) Descriptor() ([]byte, []int) {
	return file_pkg_proto_icas_icas_proto_rawDescGZIP(), []int{2}
}

func (x *GetReferenceRequest) GetInstanceName() string {
	if x != nil {
		return x.InstanceName
	}
	return ""
}

func (x *GetReferenceRequest) GetDigest() *v2.Digest {
	if x != nil {
		return x.Digest
	}
	return nil
}

func (x *GetReferenceRequest) GetDigestFunction() v2.DigestFunction_Value {
	if x != nil {
		return x.DigestFunction
	}
	return v2.DigestFunction_Value(0)
}

type Reference_S3 struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Bucket string `protobuf:"bytes,1,opt,name=bucket,proto3" json:"bucket,omitempty"`
	Key    string `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *Reference_S3) Reset() {
	*x = Reference_S3{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_proto_icas_icas_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Reference_S3) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Reference_S3) ProtoMessage() {}

func (x *Reference_S3) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_icas_icas_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Reference_S3.ProtoReflect.Descriptor instead.
func (*Reference_S3) Descriptor() ([]byte, []int) {
	return file_pkg_proto_icas_icas_proto_rawDescGZIP(), []int{0, 0}
}

func (x *Reference_S3) GetBucket() string {
	if x != nil {
		return x.Bucket
	}
	return ""
}

func (x *Reference_S3) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

type Reference_GCS struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Bucket string `protobuf:"bytes,1,opt,name=bucket,proto3" json:"bucket,omitempty"`
	Object string `protobuf:"bytes,2,opt,name=object,proto3" json:"object,omitempty"`
}

func (x *Reference_GCS) Reset() {
	*x = Reference_GCS{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_proto_icas_icas_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Reference_GCS) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Reference_GCS) ProtoMessage() {}

func (x *Reference_GCS) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_icas_icas_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Reference_GCS.ProtoReflect.Descriptor instead.
func (*Reference_GCS) Descriptor() ([]byte, []int) {
	return file_pkg_proto_icas_icas_proto_rawDescGZIP(), []int{0, 1}
}

func (x *Reference_GCS) GetBucket() string {
	if x != nil {
		return x.Bucket
	}
	return ""
}

func (x *Reference_GCS) GetObject() string {
	if x != nil {
		return x.Object
	}
	return ""
}

type Reference_ContentAddressableStorage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	InstanceName   string                  `protobuf:"bytes,1,opt,name=instance_name,json=instanceName,proto3" json:"instance_name,omitempty"`
	DigestFunction v2.DigestFunction_Value `protobuf:"varint,2,opt,name=digest_function,json=digestFunction,proto3,enum=build.bazel.remote.execution.v2.DigestFunction_Value" json:"digest_function,omitempty"`
	BlobDigest     *v2.Digest              `protobuf:"bytes,3,opt,name=blob_digest,json=blobDigest,proto3" json:"blob_digest,omitempty"`
}

func (x *Reference_ContentAddressableStorage) Reset() {
	*x = Reference_ContentAddressableStorage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_proto_icas_icas_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Reference_ContentAddressableStorage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Reference_ContentAddressableStorage) ProtoMessage() {}

func (x *Reference_ContentAddressableStorage) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_icas_icas_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Reference_ContentAddressableStorage.ProtoReflect.Descriptor instead.
func (*Reference_ContentAddressableStorage) Descriptor() ([]byte, []int) {
	return file_pkg_proto_icas_icas_proto_rawDescGZIP(), []int{0, 2}
}

func (x *Reference_ContentAddressableStorage) GetInstanceName() string {
	if x != nil {
		return x.InstanceName
	}
	return ""
}

func (x *Reference_ContentAddressableStorage) GetDigestFunction() v2.DigestFunction_Value {
	if x != nil {
		return x.DigestFunction
	}
	return v2.DigestFunction_Value(0)
}

func (x *Reference_ContentAddressableStorage) GetBlobDigest() *v2.Digest {
	if x != nil {
		return x.BlobDigest
	}
	return nil
}

type BatchUpdateReferencesRequest_Request struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Digest    *v2.Digest `protobuf:"bytes,1,opt,name=digest,proto3" json:"digest,omitempty"`
	Reference *Reference `protobuf:"bytes,2,opt,name=reference,proto3" json:"reference,omitempty"`
}

func (x *BatchUpdateReferencesRequest_Request) Reset() {
	*x = BatchUpdateReferencesRequest_Request{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_proto_icas_icas_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchUpdateReferencesRequest_Request) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchUpdateReferencesRequest_Request) ProtoMessage() {}

func (x *BatchUpdateReferencesRequest_Request) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_icas_icas_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchUpdateReferencesRequest_Request.ProtoReflect.Descriptor instead.
func (*BatchUpdateReferencesRequest_Request) Descriptor() ([]byte, []int) {
	return file_pkg_proto_icas_icas_proto_rawDescGZIP(), []int{1, 0}
}

func (x *BatchUpdateReferencesRequest_Request) GetDigest() *v2.Digest {
	if x != nil {
		return x.Digest
	}
	return nil
}

func (x *BatchUpdateReferencesRequest_Request) GetReference() *Reference {
	if x != nil {
		return x.Reference
	}
	return nil
}

var File_pkg_proto_icas_icas_proto protoreflect.FileDescriptor

var file_pkg_proto_icas_icas_proto_rawDesc = []byte{
	0x0a, 0x19, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x69, 0x63, 0x61, 0x73,
	0x2f, 0x69, 0x63, 0x61, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0e, 0x62, 0x75, 0x69,
	0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x69, 0x63, 0x61, 0x73, 0x1a, 0x36, 0x62, 0x75, 0x69,
	0x6c, 0x64, 0x2f, 0x62, 0x61, 0x7a, 0x65, 0x6c, 0x2f, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2f,
	0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x76, 0x32, 0x2f, 0x72, 0x65, 0x6d,
	0x6f, 0x74, 0x65, 0x5f, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x22, 0xff, 0x05, 0x0a, 0x09, 0x52, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63,
	0x65, 0x12, 0x1b, 0x0a, 0x08, 0x68, 0x74, 0x74, 0x70, 0x5f, 0x75, 0x72, 0x6c, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x07, 0x68, 0x74, 0x74, 0x70, 0x55, 0x72, 0x6c, 0x12, 0x2e,
	0x0a, 0x02, 0x73, 0x33, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x62, 0x75, 0x69,
	0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x69, 0x63, 0x61, 0x73, 0x2e, 0x52, 0x65, 0x66, 0x65,
	0x72, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x53, 0x33, 0x48, 0x00, 0x52, 0x02, 0x73, 0x33, 0x12, 0x31,
	0x0a, 0x03, 0x67, 0x63, 0x73, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x62, 0x75,
	0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x69, 0x63, 0x61, 0x73, 0x2e, 0x52, 0x65, 0x66,
	0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x47, 0x43, 0x53, 0x48, 0x00, 0x52, 0x03, 0x67, 0x63,
	0x73, 0x12, 0x75, 0x0a, 0x1b, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x5f, 0x61, 0x64, 0x64,
	0x72, 0x65, 0x73, 0x73, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65,
	0x18, 0x08, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x33, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61,
	0x72, 0x6e, 0x2e, 0x69, 0x63, 0x61, 0x73, 0x2e, 0x52, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63,
	0x65, 0x2e, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73,
	0x61, 0x62, 0x6c, 0x65, 0x53, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x48, 0x00, 0x52, 0x19, 0x63,
	0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x61, 0x62, 0x6c,
	0x65, 0x53, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x12, 0x21, 0x0a, 0x0c, 0x6f, 0x66, 0x66, 0x73,
	0x65, 0x74, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b,
	0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x42, 0x79, 0x74, 0x65, 0x73, 0x12, 0x1d, 0x0a, 0x0a, 0x73,
	0x69, 0x7a, 0x65, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x09, 0x73, 0x69, 0x7a, 0x65, 0x42, 0x79, 0x74, 0x65, 0x73, 0x12, 0x55, 0x0a, 0x0c, 0x64, 0x65,
	0x63, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0e,
	0x32, 0x31, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x2e, 0x62, 0x61, 0x7a, 0x65, 0x6c, 0x2e, 0x72,
	0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x76, 0x32, 0x2e, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x2e, 0x56, 0x61,
	0x6c, 0x75, 0x65, 0x52, 0x0c, 0x64, 0x65, 0x63, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f,
	0x72, 0x1a, 0x2e, 0x0a, 0x02, 0x53, 0x33, 0x12, 0x16, 0x0a, 0x06, 0x62, 0x75, 0x63, 0x6b, 0x65,
	0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x12,
	0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65,
	0x79, 0x1a, 0x35, 0x0a, 0x03, 0x47, 0x43, 0x53, 0x12, 0x16, 0x0a, 0x06, 0x62, 0x75, 0x63, 0x6b,
	0x65, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74,
	0x12, 0x16, 0x0a, 0x06, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x06, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x1a, 0xea, 0x01, 0x0a, 0x19, 0x43, 0x6f, 0x6e,
	0x74, 0x65, 0x6e, 0x74, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x61, 0x62, 0x6c, 0x65, 0x53,
	0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x12, 0x23, 0x0a, 0x0d, 0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e,
	0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x69,
	0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x5e, 0x0a, 0x0f, 0x64,
	0x69, 0x67, 0x65, 0x73, 0x74, 0x5f, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0e, 0x32, 0x35, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x2e, 0x62, 0x61, 0x7a,
	0x65, 0x6c, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74,
	0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x32, 0x2e, 0x44, 0x69, 0x67, 0x65, 0x73, 0x74, 0x46, 0x75, 0x6e,
	0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x0e, 0x64, 0x69, 0x67,
	0x65, 0x73, 0x74, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x48, 0x0a, 0x0b, 0x62,
	0x6c, 0x6f, 0x62, 0x5f, 0x64, 0x69, 0x67, 0x65, 0x73, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x27, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x2e, 0x62, 0x61, 0x7a, 0x65, 0x6c, 0x2e, 0x72,
	0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x76, 0x32, 0x2e, 0x44, 0x69, 0x67, 0x65, 0x73, 0x74, 0x52, 0x0a, 0x62, 0x6c, 0x6f, 0x62, 0x44,
	0x69, 0x67, 0x65, 0x73, 0x74, 0x42, 0x08, 0x0a, 0x06, 0x6d, 0x65, 0x64, 0x69, 0x75, 0x6d, 0x4a,
	0x04, 0x08, 0x05, 0x10, 0x06, 0x22, 0xfb, 0x02, 0x0a, 0x1c, 0x42, 0x61, 0x74, 0x63, 0x68, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x52, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x73, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x23, 0x0a, 0x0d, 0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e,
	0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x69,
	0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x50, 0x0a, 0x08, 0x72,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x34, 0x2e,
	0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x69, 0x63, 0x61, 0x73, 0x2e, 0x42,
	0x61, 0x74, 0x63, 0x68, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x52, 0x65, 0x66, 0x65, 0x72, 0x65,
	0x6e, 0x63, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x52, 0x08, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x73, 0x12, 0x5e, 0x0a,
	0x0f, 0x64, 0x69, 0x67, 0x65, 0x73, 0x74, 0x5f, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x35, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x2e, 0x62,
	0x61, 0x7a, 0x65, 0x6c, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2e, 0x65, 0x78, 0x65, 0x63,
	0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x32, 0x2e, 0x44, 0x69, 0x67, 0x65, 0x73, 0x74, 0x46,
	0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x0e, 0x64,
	0x69, 0x67, 0x65, 0x73, 0x74, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x1a, 0x83, 0x01,
	0x0a, 0x07, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x3f, 0x0a, 0x06, 0x64, 0x69, 0x67,
	0x65, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x27, 0x2e, 0x62, 0x75, 0x69, 0x6c,
	0x64, 0x2e, 0x62, 0x61, 0x7a, 0x65, 0x6c, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2e, 0x65,
	0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x32, 0x2e, 0x44, 0x69, 0x67, 0x65,
	0x73, 0x74, 0x52, 0x06, 0x64, 0x69, 0x67, 0x65, 0x73, 0x74, 0x12, 0x37, 0x0a, 0x09, 0x72, 0x65,
	0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e,
	0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x69, 0x63, 0x61, 0x73, 0x2e, 0x52,
	0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x52, 0x09, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65,
	0x6e, 0x63, 0x65, 0x22, 0xdb, 0x01, 0x0a, 0x13, 0x47, 0x65, 0x74, 0x52, 0x65, 0x66, 0x65, 0x72,
	0x65, 0x6e, 0x63, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x23, 0x0a, 0x0d, 0x69,
	0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0c, 0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65,
	0x12, 0x3f, 0x0a, 0x06, 0x64, 0x69, 0x67, 0x65, 0x73, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x27, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x2e, 0x62, 0x61, 0x7a, 0x65, 0x6c, 0x2e, 0x72,
	0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x76, 0x32, 0x2e, 0x44, 0x69, 0x67, 0x65, 0x73, 0x74, 0x52, 0x06, 0x64, 0x69, 0x67, 0x65, 0x73,
	0x74, 0x12, 0x5e, 0x0a, 0x0f, 0x64, 0x69, 0x67, 0x65, 0x73, 0x74, 0x5f, 0x66, 0x75, 0x6e, 0x63,
	0x74, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x35, 0x2e, 0x62, 0x75, 0x69,
	0x6c, 0x64, 0x2e, 0x62, 0x61, 0x7a, 0x65, 0x6c, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2e,
	0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x32, 0x2e, 0x44, 0x69, 0x67,
	0x65, 0x73, 0x74, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x52, 0x0e, 0x64, 0x69, 0x67, 0x65, 0x73, 0x74, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x32, 0x85, 0x03, 0x0a, 0x21, 0x49, 0x6e, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x43, 0x6f,
	0x6e, 0x74, 0x65, 0x6e, 0x74, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x61, 0x62, 0x6c, 0x65,
	0x53, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x12, 0x8c, 0x01, 0x0a, 0x15, 0x46, 0x69, 0x6e, 0x64,
	0x4d, 0x69, 0x73, 0x73, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65,
	0x73, 0x12, 0x38, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x2e, 0x62, 0x61, 0x7a, 0x65, 0x6c, 0x2e,
	0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e,
	0x2e, 0x76, 0x32, 0x2e, 0x46, 0x69, 0x6e, 0x64, 0x4d, 0x69, 0x73, 0x73, 0x69, 0x6e, 0x67, 0x42,
	0x6c, 0x6f, 0x62, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x39, 0x2e, 0x62, 0x75,
	0x69, 0x6c, 0x64, 0x2e, 0x62, 0x61, 0x7a, 0x65, 0x6c, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65,
	0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x32, 0x2e, 0x46, 0x69,
	0x6e, 0x64, 0x4d, 0x69, 0x73, 0x73, 0x69, 0x6e, 0x67, 0x42, 0x6c, 0x6f, 0x62, 0x73, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x80, 0x01, 0x0a, 0x15, 0x42, 0x61, 0x74, 0x63, 0x68,
	0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x52, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x73,
	0x12, 0x2c, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x69, 0x63, 0x61,
	0x73, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x52, 0x65, 0x66,
	0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x39,
	0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x2e, 0x62, 0x61, 0x7a, 0x65, 0x6c, 0x2e, 0x72, 0x65, 0x6d,
	0x6f, 0x74, 0x65, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x32,
	0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x42, 0x6c, 0x6f, 0x62,
	0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x4e, 0x0a, 0x0c, 0x47, 0x65, 0x74,
	0x52, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x12, 0x23, 0x2e, 0x62, 0x75, 0x69, 0x6c,
	0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x69, 0x63, 0x61, 0x73, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65,
	0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19,
	0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x69, 0x63, 0x61, 0x73, 0x2e,
	0x52, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x42, 0x30, 0x5a, 0x2e, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72,
	0x6e, 0x2f, 0x62, 0x62, 0x2d, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2f, 0x70, 0x6b, 0x67,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x69, 0x63, 0x61, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_pkg_proto_icas_icas_proto_rawDescOnce sync.Once
	file_pkg_proto_icas_icas_proto_rawDescData = file_pkg_proto_icas_icas_proto_rawDesc
)

func file_pkg_proto_icas_icas_proto_rawDescGZIP() []byte {
	file_pkg_proto_icas_icas_proto_rawDescOnce.Do(func() {
		file_pkg_proto_icas_icas_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_proto_icas_icas_proto_rawDescData)
	})
	return file_pkg_proto_icas_icas_proto_rawDescData
}

var file_pkg_proto_icas_icas_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_pkg_proto_icas_icas_proto_goTypes = []interface{}{
	(*Reference)(nil),                            // 0: buildbarn.icas.Reference
	(*BatchUpdateReferencesRequest)(nil),         // 1: buildbarn.icas.BatchUpdateReferencesRequest
	(*GetReferenceRequest)(nil),                  // 2: buildbarn.icas.GetReferenceRequest
	(*Reference_S3)(nil),                         // 3: buildbarn.icas.Reference.S3
	(*Reference_GCS)(nil),                        // 4: buildbarn.icas.Reference.GCS
	(*Reference_ContentAddressableStorage)(nil),  // 5: buildbarn.icas.Reference.ContentAddressableStorage
	(*BatchUpdateReferencesRequest_Request)(nil), // 6: buildbarn.icas.BatchUpdateReferencesRequest.Request
	(v2.Compressor_Value)(0),                     // 7: build.bazel.remote.execution.v2.Compressor.Value
	(v2.DigestFunction_Value)(0),                 // 8: build.bazel.remote.execution.v2.DigestFunction.Value
	(*v2.Digest)(nil),                            // 9: build.bazel.remote.execution.v2.Digest
	(*v2.FindMissingBlobsRequest)(nil),           // 10: build.bazel.remote.execution.v2.FindMissingBlobsRequest
	(*v2.FindMissingBlobsResponse)(nil),          // 11: build.bazel.remote.execution.v2.FindMissingBlobsResponse
	(*v2.BatchUpdateBlobsResponse)(nil),          // 12: build.bazel.remote.execution.v2.BatchUpdateBlobsResponse
}
var file_pkg_proto_icas_icas_proto_depIdxs = []int32{
	3,  // 0: buildbarn.icas.Reference.s3:type_name -> buildbarn.icas.Reference.S3
	4,  // 1: buildbarn.icas.Reference.gcs:type_name -> buildbarn.icas.Reference.GCS
	5,  // 2: buildbarn.icas.Reference.content_addressable_storage:type_name -> buildbarn.icas.Reference.ContentAddressableStorage
	7,  // 3: buildbarn.icas.Reference.decompressor:type_name -> build.bazel.remote.execution.v2.Compressor.Value
	6,  // 4: buildbarn.icas.BatchUpdateReferencesRequest.requests:type_name -> buildbarn.icas.BatchUpdateReferencesRequest.Request
	8,  // 5: buildbarn.icas.BatchUpdateReferencesRequest.digest_function:type_name -> build.bazel.remote.execution.v2.DigestFunction.Value
	9,  // 6: buildbarn.icas.GetReferenceRequest.digest:type_name -> build.bazel.remote.execution.v2.Digest
	8,  // 7: buildbarn.icas.GetReferenceRequest.digest_function:type_name -> build.bazel.remote.execution.v2.DigestFunction.Value
	8,  // 8: buildbarn.icas.Reference.ContentAddressableStorage.digest_function:type_name -> build.bazel.remote.execution.v2.DigestFunction.Value
	9,  // 9: buildbarn.icas.Reference.ContentAddressableStorage.blob_digest:type_name -> build.bazel.remote.execution.v2.Digest
	9,  // 10: buildbarn.icas.BatchUpdateReferencesRequest.Request.digest:type_name -> build.bazel.remote.execution.v2.Digest
	0,  // 11: buildbarn.icas.BatchUpdateReferencesRequest.Request.reference:type_name -> buildbarn.icas.Reference
	10, // 12: buildbarn.icas.IndirectContentAddressableStorage.FindMissingReferences:input_type -> build.bazel.remote.execution.v2.FindMissingBlobsRequest
	1,  // 13: buildbarn.icas.IndirectContentAddressableStorage.BatchUpdateReferences:input_type -> buildbarn.icas.BatchUpdateReferencesRequest
	2,  // 14: buildbarn.icas.IndirectContentAddressableStorage.GetReference:input_type -> buildbarn.icas.GetReferenceRequest
	11, // 15: buildbarn.icas.IndirectContentAddressableStorage.FindMissingReferences:output_type -> build.bazel.remote.execution.v2.FindMissingBlobsResponse
	12, // 16: buildbarn.icas.IndirectContentAddressableStorage.BatchUpdateReferences:output_type -> build.bazel.remote.execution.v2.BatchUpdateBlobsResponse
	0,  // 17: buildbarn.icas.IndirectContentAddressableStorage.GetReference:output_type -> buildbarn.icas.Reference
	15, // [15:18] is the sub-list for method output_type
	12, // [12:15] is the sub-list for method input_type
	12, // [12:12] is the sub-list for extension type_name
	12, // [12:12] is the sub-list for extension extendee
	0,  // [0:12] is the sub-list for field type_name
}

func init() { file_pkg_proto_icas_icas_proto_init() }
func file_pkg_proto_icas_icas_proto_init() {
	if File_pkg_proto_icas_icas_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_proto_icas_icas_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Reference); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_proto_icas_icas_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchUpdateReferencesRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_proto_icas_icas_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetReferenceRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_proto_icas_icas_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Reference_S3); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_proto_icas_icas_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Reference_GCS); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_proto_icas_icas_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Reference_ContentAddressableStorage); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_proto_icas_icas_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchUpdateReferencesRequest_Request); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_pkg_proto_icas_icas_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*Reference_HttpUrl)(nil),
		(*Reference_S3_)(nil),
		(*Reference_Gcs)(nil),
		(*Reference_ContentAddressableStorage_)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_proto_icas_icas_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_proto_icas_icas_proto_goTypes,
		DependencyIndexes: file_pkg_proto_icas_icas_proto_depIdxs,
		MessageInfos:      file_pkg_proto_icas_icas_proto_msgTypes,
	}.Build()
	File_pkg_proto_icas_icas_proto = out.File
	file_pkg_proto_icas_icas_proto_rawDesc = nil
	file_pkg_proto_icas_icas_proto_goTypes = nil
	file_pkg_proto_icas_icas_proto_depIdxs = nil
}
