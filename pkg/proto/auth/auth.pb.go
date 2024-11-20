// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.2
// 	protoc        v5.28.3
// source: pkg/proto/auth/auth.proto

package auth

import (
	v1 "go.opentelemetry.io/proto/otlp/common/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	structpb "google.golang.org/protobuf/types/known/structpb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type AuthenticationMetadata struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Public            *structpb.Value `protobuf:"bytes,1,opt,name=public,proto3" json:"public,omitempty"`
	TracingAttributes []*v1.KeyValue  `protobuf:"bytes,2,rep,name=tracing_attributes,json=tracingAttributes,proto3" json:"tracing_attributes,omitempty"`
	Private           *structpb.Value `protobuf:"bytes,3,opt,name=private,proto3" json:"private,omitempty"`
}

func (x *AuthenticationMetadata) Reset() {
	*x = AuthenticationMetadata{}
	mi := &file_pkg_proto_auth_auth_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AuthenticationMetadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AuthenticationMetadata) ProtoMessage() {}

func (x *AuthenticationMetadata) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_auth_auth_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AuthenticationMetadata.ProtoReflect.Descriptor instead.
func (*AuthenticationMetadata) Descriptor() ([]byte, []int) {
	return file_pkg_proto_auth_auth_proto_rawDescGZIP(), []int{0}
}

func (x *AuthenticationMetadata) GetPublic() *structpb.Value {
	if x != nil {
		return x.Public
	}
	return nil
}

func (x *AuthenticationMetadata) GetTracingAttributes() []*v1.KeyValue {
	if x != nil {
		return x.TracingAttributes
	}
	return nil
}

func (x *AuthenticationMetadata) GetPrivate() *structpb.Value {
	if x != nil {
		return x.Private
	}
	return nil
}

var File_pkg_proto_auth_auth_proto protoreflect.FileDescriptor

var file_pkg_proto_auth_auth_proto_rawDesc = []byte{
	0x0a, 0x19, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x61, 0x75, 0x74, 0x68,
	0x2f, 0x61, 0x75, 0x74, 0x68, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0e, 0x62, 0x75, 0x69,
	0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x61, 0x75, 0x74, 0x68, 0x1a, 0x1c, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x73, 0x74, 0x72,
	0x75, 0x63, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x2a, 0x6f, 0x70, 0x65, 0x6e, 0x74,
	0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63,
	0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x76, 0x31, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xd2, 0x01, 0x0a, 0x16, 0x41, 0x75, 0x74, 0x68, 0x65, 0x6e,
	0x74, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61,
	0x12, 0x2e, 0x0a, 0x06, 0x70, 0x75, 0x62, 0x6c, 0x69, 0x63, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2e, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x06, 0x70, 0x75, 0x62, 0x6c, 0x69, 0x63,
	0x12, 0x56, 0x0a, 0x12, 0x74, 0x72, 0x61, 0x63, 0x69, 0x6e, 0x67, 0x5f, 0x61, 0x74, 0x74, 0x72,
	0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x27, 0x2e, 0x6f,
	0x70, 0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x4b, 0x65, 0x79,
	0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x11, 0x74, 0x72, 0x61, 0x63, 0x69, 0x6e, 0x67, 0x41, 0x74,
	0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x12, 0x30, 0x0a, 0x07, 0x70, 0x72, 0x69, 0x76,
	0x61, 0x74, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x52, 0x07, 0x70, 0x72, 0x69, 0x76, 0x61, 0x74, 0x65, 0x42, 0x30, 0x5a, 0x2e, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61,
	0x72, 0x6e, 0x2f, 0x62, 0x62, 0x2d, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2f, 0x70, 0x6b,
	0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x61, 0x75, 0x74, 0x68, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_proto_auth_auth_proto_rawDescOnce sync.Once
	file_pkg_proto_auth_auth_proto_rawDescData = file_pkg_proto_auth_auth_proto_rawDesc
)

func file_pkg_proto_auth_auth_proto_rawDescGZIP() []byte {
	file_pkg_proto_auth_auth_proto_rawDescOnce.Do(func() {
		file_pkg_proto_auth_auth_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_proto_auth_auth_proto_rawDescData)
	})
	return file_pkg_proto_auth_auth_proto_rawDescData
}

var file_pkg_proto_auth_auth_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_pkg_proto_auth_auth_proto_goTypes = []any{
	(*AuthenticationMetadata)(nil), // 0: buildbarn.auth.AuthenticationMetadata
	(*structpb.Value)(nil),         // 1: google.protobuf.Value
	(*v1.KeyValue)(nil),            // 2: opentelemetry.proto.common.v1.KeyValue
}
var file_pkg_proto_auth_auth_proto_depIdxs = []int32{
	1, // 0: buildbarn.auth.AuthenticationMetadata.public:type_name -> google.protobuf.Value
	2, // 1: buildbarn.auth.AuthenticationMetadata.tracing_attributes:type_name -> opentelemetry.proto.common.v1.KeyValue
	1, // 2: buildbarn.auth.AuthenticationMetadata.private:type_name -> google.protobuf.Value
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_pkg_proto_auth_auth_proto_init() }
func file_pkg_proto_auth_auth_proto_init() {
	if File_pkg_proto_auth_auth_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_proto_auth_auth_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_proto_auth_auth_proto_goTypes,
		DependencyIndexes: file_pkg_proto_auth_auth_proto_depIdxs,
		MessageInfos:      file_pkg_proto_auth_auth_proto_msgTypes,
	}.Build()
	File_pkg_proto_auth_auth_proto = out.File
	file_pkg_proto_auth_auth_proto_rawDesc = nil
	file_pkg_proto_auth_auth_proto_goTypes = nil
	file_pkg_proto_auth_auth_proto_depIdxs = nil
}
