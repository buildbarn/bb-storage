// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v6.31.1
// source: pkg/proto/configuration/cloud/aws/aws.proto

package aws

import (
	http "github.com/buildbarn/bb-storage/pkg/proto/configuration/http"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
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

type StaticCredentials struct {
	state           protoimpl.MessageState `protogen:"open.v1"`
	AccessKeyId     string                 `protobuf:"bytes,1,opt,name=access_key_id,json=accessKeyId,proto3" json:"access_key_id,omitempty"`
	SecretAccessKey string                 `protobuf:"bytes,2,opt,name=secret_access_key,json=secretAccessKey,proto3" json:"secret_access_key,omitempty"`
	unknownFields   protoimpl.UnknownFields
	sizeCache       protoimpl.SizeCache
}

func (x *StaticCredentials) Reset() {
	*x = StaticCredentials{}
	mi := &file_pkg_proto_configuration_cloud_aws_aws_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StaticCredentials) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StaticCredentials) ProtoMessage() {}

func (x *StaticCredentials) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_configuration_cloud_aws_aws_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StaticCredentials.ProtoReflect.Descriptor instead.
func (*StaticCredentials) Descriptor() ([]byte, []int) {
	return file_pkg_proto_configuration_cloud_aws_aws_proto_rawDescGZIP(), []int{0}
}

func (x *StaticCredentials) GetAccessKeyId() string {
	if x != nil {
		return x.AccessKeyId
	}
	return ""
}

func (x *StaticCredentials) GetSecretAccessKey() string {
	if x != nil {
		return x.SecretAccessKey
	}
	return ""
}

type WebIdentityRoleCredentials struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	RoleArn       string                 `protobuf:"bytes,1,opt,name=role_arn,json=roleArn,proto3" json:"role_arn,omitempty"`
	TokenFile     string                 `protobuf:"bytes,2,opt,name=token_file,json=tokenFile,proto3" json:"token_file,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *WebIdentityRoleCredentials) Reset() {
	*x = WebIdentityRoleCredentials{}
	mi := &file_pkg_proto_configuration_cloud_aws_aws_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WebIdentityRoleCredentials) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WebIdentityRoleCredentials) ProtoMessage() {}

func (x *WebIdentityRoleCredentials) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_configuration_cloud_aws_aws_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WebIdentityRoleCredentials.ProtoReflect.Descriptor instead.
func (*WebIdentityRoleCredentials) Descriptor() ([]byte, []int) {
	return file_pkg_proto_configuration_cloud_aws_aws_proto_rawDescGZIP(), []int{1}
}

func (x *WebIdentityRoleCredentials) GetRoleArn() string {
	if x != nil {
		return x.RoleArn
	}
	return ""
}

func (x *WebIdentityRoleCredentials) GetTokenFile() string {
	if x != nil {
		return x.TokenFile
	}
	return ""
}

type SessionConfiguration struct {
	state  protoimpl.MessageState `protogen:"open.v1"`
	Region string                 `protobuf:"bytes,2,opt,name=region,proto3" json:"region,omitempty"`
	// Types that are valid to be assigned to Credentials:
	//
	//	*SessionConfiguration_StaticCredentials
	//	*SessionConfiguration_WebIdentityRoleCredentials
	Credentials   isSessionConfiguration_Credentials `protobuf_oneof:"credentials"`
	HttpClient    *http.ClientConfiguration          `protobuf:"bytes,6,opt,name=http_client,json=httpClient,proto3" json:"http_client,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SessionConfiguration) Reset() {
	*x = SessionConfiguration{}
	mi := &file_pkg_proto_configuration_cloud_aws_aws_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SessionConfiguration) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SessionConfiguration) ProtoMessage() {}

func (x *SessionConfiguration) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_configuration_cloud_aws_aws_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SessionConfiguration.ProtoReflect.Descriptor instead.
func (*SessionConfiguration) Descriptor() ([]byte, []int) {
	return file_pkg_proto_configuration_cloud_aws_aws_proto_rawDescGZIP(), []int{2}
}

func (x *SessionConfiguration) GetRegion() string {
	if x != nil {
		return x.Region
	}
	return ""
}

func (x *SessionConfiguration) GetCredentials() isSessionConfiguration_Credentials {
	if x != nil {
		return x.Credentials
	}
	return nil
}

func (x *SessionConfiguration) GetStaticCredentials() *StaticCredentials {
	if x != nil {
		if x, ok := x.Credentials.(*SessionConfiguration_StaticCredentials); ok {
			return x.StaticCredentials
		}
	}
	return nil
}

func (x *SessionConfiguration) GetWebIdentityRoleCredentials() *WebIdentityRoleCredentials {
	if x != nil {
		if x, ok := x.Credentials.(*SessionConfiguration_WebIdentityRoleCredentials); ok {
			return x.WebIdentityRoleCredentials
		}
	}
	return nil
}

func (x *SessionConfiguration) GetHttpClient() *http.ClientConfiguration {
	if x != nil {
		return x.HttpClient
	}
	return nil
}

type isSessionConfiguration_Credentials interface {
	isSessionConfiguration_Credentials()
}

type SessionConfiguration_StaticCredentials struct {
	StaticCredentials *StaticCredentials `protobuf:"bytes,5,opt,name=static_credentials,json=staticCredentials,proto3,oneof"`
}

type SessionConfiguration_WebIdentityRoleCredentials struct {
	WebIdentityRoleCredentials *WebIdentityRoleCredentials `protobuf:"bytes,7,opt,name=web_identity_role_credentials,json=webIdentityRoleCredentials,proto3,oneof"`
}

func (*SessionConfiguration_StaticCredentials) isSessionConfiguration_Credentials() {}

func (*SessionConfiguration_WebIdentityRoleCredentials) isSessionConfiguration_Credentials() {}

var File_pkg_proto_configuration_cloud_aws_aws_proto protoreflect.FileDescriptor

const file_pkg_proto_configuration_cloud_aws_aws_proto_rawDesc = "" +
	"\n" +
	"+pkg/proto/configuration/cloud/aws/aws.proto\x12!buildbarn.configuration.cloud.aws\x1a'pkg/proto/configuration/http/http.proto\"c\n" +
	"\x11StaticCredentials\x12\"\n" +
	"\raccess_key_id\x18\x01 \x01(\tR\vaccessKeyId\x12*\n" +
	"\x11secret_access_key\x18\x02 \x01(\tR\x0fsecretAccessKey\"V\n" +
	"\x1aWebIdentityRoleCredentials\x12\x19\n" +
	"\brole_arn\x18\x01 \x01(\tR\aroleArn\x12\x1d\n" +
	"\n" +
	"token_file\x18\x02 \x01(\tR\ttokenFile\"\x8f\x03\n" +
	"\x14SessionConfiguration\x12\x16\n" +
	"\x06region\x18\x02 \x01(\tR\x06region\x12e\n" +
	"\x12static_credentials\x18\x05 \x01(\v24.buildbarn.configuration.cloud.aws.StaticCredentialsH\x00R\x11staticCredentials\x12\x82\x01\n" +
	"\x1dweb_identity_role_credentials\x18\a \x01(\v2=.buildbarn.configuration.cloud.aws.WebIdentityRoleCredentialsH\x00R\x1awebIdentityRoleCredentials\x12R\n" +
	"\vhttp_client\x18\x06 \x01(\v21.buildbarn.configuration.http.ClientConfigurationR\n" +
	"httpClientB\r\n" +
	"\vcredentialsJ\x04\b\x01\x10\x02J\x04\b\x03\x10\x04J\x04\b\x04\x10\x05BCZAgithub.com/buildbarn/bb-storage/pkg/proto/configuration/cloud/awsb\x06proto3"

var (
	file_pkg_proto_configuration_cloud_aws_aws_proto_rawDescOnce sync.Once
	file_pkg_proto_configuration_cloud_aws_aws_proto_rawDescData []byte
)

func file_pkg_proto_configuration_cloud_aws_aws_proto_rawDescGZIP() []byte {
	file_pkg_proto_configuration_cloud_aws_aws_proto_rawDescOnce.Do(func() {
		file_pkg_proto_configuration_cloud_aws_aws_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_pkg_proto_configuration_cloud_aws_aws_proto_rawDesc), len(file_pkg_proto_configuration_cloud_aws_aws_proto_rawDesc)))
	})
	return file_pkg_proto_configuration_cloud_aws_aws_proto_rawDescData
}

var file_pkg_proto_configuration_cloud_aws_aws_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_pkg_proto_configuration_cloud_aws_aws_proto_goTypes = []any{
	(*StaticCredentials)(nil),          // 0: buildbarn.configuration.cloud.aws.StaticCredentials
	(*WebIdentityRoleCredentials)(nil), // 1: buildbarn.configuration.cloud.aws.WebIdentityRoleCredentials
	(*SessionConfiguration)(nil),       // 2: buildbarn.configuration.cloud.aws.SessionConfiguration
	(*http.ClientConfiguration)(nil),   // 3: buildbarn.configuration.http.ClientConfiguration
}
var file_pkg_proto_configuration_cloud_aws_aws_proto_depIdxs = []int32{
	0, // 0: buildbarn.configuration.cloud.aws.SessionConfiguration.static_credentials:type_name -> buildbarn.configuration.cloud.aws.StaticCredentials
	1, // 1: buildbarn.configuration.cloud.aws.SessionConfiguration.web_identity_role_credentials:type_name -> buildbarn.configuration.cloud.aws.WebIdentityRoleCredentials
	3, // 2: buildbarn.configuration.cloud.aws.SessionConfiguration.http_client:type_name -> buildbarn.configuration.http.ClientConfiguration
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_pkg_proto_configuration_cloud_aws_aws_proto_init() }
func file_pkg_proto_configuration_cloud_aws_aws_proto_init() {
	if File_pkg_proto_configuration_cloud_aws_aws_proto != nil {
		return
	}
	file_pkg_proto_configuration_cloud_aws_aws_proto_msgTypes[2].OneofWrappers = []any{
		(*SessionConfiguration_StaticCredentials)(nil),
		(*SessionConfiguration_WebIdentityRoleCredentials)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_pkg_proto_configuration_cloud_aws_aws_proto_rawDesc), len(file_pkg_proto_configuration_cloud_aws_aws_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_proto_configuration_cloud_aws_aws_proto_goTypes,
		DependencyIndexes: file_pkg_proto_configuration_cloud_aws_aws_proto_depIdxs,
		MessageInfos:      file_pkg_proto_configuration_cloud_aws_aws_proto_msgTypes,
	}.Build()
	File_pkg_proto_configuration_cloud_aws_aws_proto = out.File
	file_pkg_proto_configuration_cloud_aws_aws_proto_goTypes = nil
	file_pkg_proto_configuration_cloud_aws_aws_proto_depIdxs = nil
}
