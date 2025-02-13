// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        v5.29.3
// source: pkg/proto/configuration/auth/auth.proto

package auth

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type AuthorizerConfiguration struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Types that are valid to be assigned to Policy:
	//
	//	*AuthorizerConfiguration_Allow
	//	*AuthorizerConfiguration_InstanceNamePrefix
	//	*AuthorizerConfiguration_Deny
	//	*AuthorizerConfiguration_JmespathExpression
	Policy        isAuthorizerConfiguration_Policy `protobuf_oneof:"policy"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *AuthorizerConfiguration) Reset() {
	*x = AuthorizerConfiguration{}
	mi := &file_pkg_proto_configuration_auth_auth_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AuthorizerConfiguration) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AuthorizerConfiguration) ProtoMessage() {}

func (x *AuthorizerConfiguration) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_configuration_auth_auth_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AuthorizerConfiguration.ProtoReflect.Descriptor instead.
func (*AuthorizerConfiguration) Descriptor() ([]byte, []int) {
	return file_pkg_proto_configuration_auth_auth_proto_rawDescGZIP(), []int{0}
}

func (x *AuthorizerConfiguration) GetPolicy() isAuthorizerConfiguration_Policy {
	if x != nil {
		return x.Policy
	}
	return nil
}

func (x *AuthorizerConfiguration) GetAllow() *emptypb.Empty {
	if x != nil {
		if x, ok := x.Policy.(*AuthorizerConfiguration_Allow); ok {
			return x.Allow
		}
	}
	return nil
}

func (x *AuthorizerConfiguration) GetInstanceNamePrefix() *InstanceNameAuthorizer {
	if x != nil {
		if x, ok := x.Policy.(*AuthorizerConfiguration_InstanceNamePrefix); ok {
			return x.InstanceNamePrefix
		}
	}
	return nil
}

func (x *AuthorizerConfiguration) GetDeny() *emptypb.Empty {
	if x != nil {
		if x, ok := x.Policy.(*AuthorizerConfiguration_Deny); ok {
			return x.Deny
		}
	}
	return nil
}

func (x *AuthorizerConfiguration) GetJmespathExpression() string {
	if x != nil {
		if x, ok := x.Policy.(*AuthorizerConfiguration_JmespathExpression); ok {
			return x.JmespathExpression
		}
	}
	return ""
}

type isAuthorizerConfiguration_Policy interface {
	isAuthorizerConfiguration_Policy()
}

type AuthorizerConfiguration_Allow struct {
	Allow *emptypb.Empty `protobuf:"bytes,1,opt,name=allow,proto3,oneof"`
}

type AuthorizerConfiguration_InstanceNamePrefix struct {
	InstanceNamePrefix *InstanceNameAuthorizer `protobuf:"bytes,2,opt,name=instance_name_prefix,json=instanceNamePrefix,proto3,oneof"`
}

type AuthorizerConfiguration_Deny struct {
	Deny *emptypb.Empty `protobuf:"bytes,3,opt,name=deny,proto3,oneof"`
}

type AuthorizerConfiguration_JmespathExpression struct {
	JmespathExpression string `protobuf:"bytes,4,opt,name=jmespath_expression,json=jmespathExpression,proto3,oneof"`
}

func (*AuthorizerConfiguration_Allow) isAuthorizerConfiguration_Policy() {}

func (*AuthorizerConfiguration_InstanceNamePrefix) isAuthorizerConfiguration_Policy() {}

func (*AuthorizerConfiguration_Deny) isAuthorizerConfiguration_Policy() {}

func (*AuthorizerConfiguration_JmespathExpression) isAuthorizerConfiguration_Policy() {}

type InstanceNameAuthorizer struct {
	state                       protoimpl.MessageState `protogen:"open.v1"`
	AllowedInstanceNamePrefixes []string               `protobuf:"bytes,1,rep,name=allowed_instance_name_prefixes,json=allowedInstanceNamePrefixes,proto3" json:"allowed_instance_name_prefixes,omitempty"`
	unknownFields               protoimpl.UnknownFields
	sizeCache                   protoimpl.SizeCache
}

func (x *InstanceNameAuthorizer) Reset() {
	*x = InstanceNameAuthorizer{}
	mi := &file_pkg_proto_configuration_auth_auth_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InstanceNameAuthorizer) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InstanceNameAuthorizer) ProtoMessage() {}

func (x *InstanceNameAuthorizer) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_configuration_auth_auth_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InstanceNameAuthorizer.ProtoReflect.Descriptor instead.
func (*InstanceNameAuthorizer) Descriptor() ([]byte, []int) {
	return file_pkg_proto_configuration_auth_auth_proto_rawDescGZIP(), []int{1}
}

func (x *InstanceNameAuthorizer) GetAllowedInstanceNamePrefixes() []string {
	if x != nil {
		return x.AllowedInstanceNamePrefixes
	}
	return nil
}

var File_pkg_proto_configuration_auth_auth_proto protoreflect.FileDescriptor

var file_pkg_proto_configuration_auth_auth_proto_rawDesc = string([]byte{
	0x0a, 0x27, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x61, 0x75, 0x74, 0x68, 0x2f, 0x61,
	0x75, 0x74, 0x68, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1c, 0x62, 0x75, 0x69, 0x6c, 0x64,
	0x62, 0x61, 0x72, 0x6e, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x2e, 0x61, 0x75, 0x74, 0x68, 0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0x9e, 0x02, 0x0a, 0x17, 0x41, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69,
	0x7a, 0x65, 0x72, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x12, 0x2e, 0x0a, 0x05, 0x61, 0x6c, 0x6c, 0x6f, 0x77, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x48, 0x00, 0x52, 0x05, 0x61, 0x6c, 0x6c, 0x6f, 0x77,
	0x12, 0x68, 0x0a, 0x14, 0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d,
	0x65, 0x5f, 0x70, 0x72, 0x65, 0x66, 0x69, 0x78, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x34,
	0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x61, 0x75, 0x74, 0x68, 0x2e, 0x49, 0x6e,
	0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x41, 0x75, 0x74, 0x68, 0x6f, 0x72,
	0x69, 0x7a, 0x65, 0x72, 0x48, 0x00, 0x52, 0x12, 0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65,
	0x4e, 0x61, 0x6d, 0x65, 0x50, 0x72, 0x65, 0x66, 0x69, 0x78, 0x12, 0x2c, 0x0a, 0x04, 0x64, 0x65,
	0x6e, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79,
	0x48, 0x00, 0x52, 0x04, 0x64, 0x65, 0x6e, 0x79, 0x12, 0x31, 0x0a, 0x13, 0x6a, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x74, 0x68, 0x5f, 0x65, 0x78, 0x70, 0x72, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x12, 0x6a, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x74,
	0x68, 0x45, 0x78, 0x70, 0x72, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x42, 0x08, 0x0a, 0x06, 0x70,
	0x6f, 0x6c, 0x69, 0x63, 0x79, 0x22, 0x5d, 0x0a, 0x16, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63,
	0x65, 0x4e, 0x61, 0x6d, 0x65, 0x41, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x65, 0x72, 0x12,
	0x43, 0x0a, 0x1e, 0x61, 0x6c, 0x6c, 0x6f, 0x77, 0x65, 0x64, 0x5f, 0x69, 0x6e, 0x73, 0x74, 0x61,
	0x6e, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5f, 0x70, 0x72, 0x65, 0x66, 0x69, 0x78, 0x65,
	0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x1b, 0x61, 0x6c, 0x6c, 0x6f, 0x77, 0x65, 0x64,
	0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x50, 0x72, 0x65, 0x66,
	0x69, 0x78, 0x65, 0x73, 0x42, 0x3e, 0x5a, 0x3c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2f, 0x62, 0x62, 0x2d,
	0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f,
	0x61, 0x75, 0x74, 0x68, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_pkg_proto_configuration_auth_auth_proto_rawDescOnce sync.Once
	file_pkg_proto_configuration_auth_auth_proto_rawDescData []byte
)

func file_pkg_proto_configuration_auth_auth_proto_rawDescGZIP() []byte {
	file_pkg_proto_configuration_auth_auth_proto_rawDescOnce.Do(func() {
		file_pkg_proto_configuration_auth_auth_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_pkg_proto_configuration_auth_auth_proto_rawDesc), len(file_pkg_proto_configuration_auth_auth_proto_rawDesc)))
	})
	return file_pkg_proto_configuration_auth_auth_proto_rawDescData
}

var file_pkg_proto_configuration_auth_auth_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_pkg_proto_configuration_auth_auth_proto_goTypes = []any{
	(*AuthorizerConfiguration)(nil), // 0: buildbarn.configuration.auth.AuthorizerConfiguration
	(*InstanceNameAuthorizer)(nil),  // 1: buildbarn.configuration.auth.InstanceNameAuthorizer
	(*emptypb.Empty)(nil),           // 2: google.protobuf.Empty
}
var file_pkg_proto_configuration_auth_auth_proto_depIdxs = []int32{
	2, // 0: buildbarn.configuration.auth.AuthorizerConfiguration.allow:type_name -> google.protobuf.Empty
	1, // 1: buildbarn.configuration.auth.AuthorizerConfiguration.instance_name_prefix:type_name -> buildbarn.configuration.auth.InstanceNameAuthorizer
	2, // 2: buildbarn.configuration.auth.AuthorizerConfiguration.deny:type_name -> google.protobuf.Empty
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_pkg_proto_configuration_auth_auth_proto_init() }
func file_pkg_proto_configuration_auth_auth_proto_init() {
	if File_pkg_proto_configuration_auth_auth_proto != nil {
		return
	}
	file_pkg_proto_configuration_auth_auth_proto_msgTypes[0].OneofWrappers = []any{
		(*AuthorizerConfiguration_Allow)(nil),
		(*AuthorizerConfiguration_InstanceNamePrefix)(nil),
		(*AuthorizerConfiguration_Deny)(nil),
		(*AuthorizerConfiguration_JmespathExpression)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_pkg_proto_configuration_auth_auth_proto_rawDesc), len(file_pkg_proto_configuration_auth_auth_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_proto_configuration_auth_auth_proto_goTypes,
		DependencyIndexes: file_pkg_proto_configuration_auth_auth_proto_depIdxs,
		MessageInfos:      file_pkg_proto_configuration_auth_auth_proto_msgTypes,
	}.Build()
	File_pkg_proto_configuration_auth_auth_proto = out.File
	file_pkg_proto_configuration_auth_auth_proto_goTypes = nil
	file_pkg_proto_configuration_auth_auth_proto_depIdxs = nil
}
