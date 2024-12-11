// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.2
// 	protoc        v5.29.1
// source: pkg/proto/configuration/blockdevice/blockdevice.proto

package blockdevice

import (
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

type FileConfiguration struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Path      string `protobuf:"bytes,1,opt,name=path,proto3" json:"path,omitempty"`
	SizeBytes int64  `protobuf:"varint,2,opt,name=size_bytes,json=sizeBytes,proto3" json:"size_bytes,omitempty"`
}

func (x *FileConfiguration) Reset() {
	*x = FileConfiguration{}
	mi := &file_pkg_proto_configuration_blockdevice_blockdevice_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FileConfiguration) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FileConfiguration) ProtoMessage() {}

func (x *FileConfiguration) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_configuration_blockdevice_blockdevice_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FileConfiguration.ProtoReflect.Descriptor instead.
func (*FileConfiguration) Descriptor() ([]byte, []int) {
	return file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDescGZIP(), []int{0}
}

func (x *FileConfiguration) GetPath() string {
	if x != nil {
		return x.Path
	}
	return ""
}

func (x *FileConfiguration) GetSizeBytes() int64 {
	if x != nil {
		return x.SizeBytes
	}
	return 0
}

type Configuration struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Source:
	//
	//	*Configuration_DevicePath
	//	*Configuration_File
	Source isConfiguration_Source `protobuf_oneof:"source"`
}

func (x *Configuration) Reset() {
	*x = Configuration{}
	mi := &file_pkg_proto_configuration_blockdevice_blockdevice_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Configuration) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Configuration) ProtoMessage() {}

func (x *Configuration) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_configuration_blockdevice_blockdevice_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Configuration.ProtoReflect.Descriptor instead.
func (*Configuration) Descriptor() ([]byte, []int) {
	return file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDescGZIP(), []int{1}
}

func (m *Configuration) GetSource() isConfiguration_Source {
	if m != nil {
		return m.Source
	}
	return nil
}

func (x *Configuration) GetDevicePath() string {
	if x, ok := x.GetSource().(*Configuration_DevicePath); ok {
		return x.DevicePath
	}
	return ""
}

func (x *Configuration) GetFile() *FileConfiguration {
	if x, ok := x.GetSource().(*Configuration_File); ok {
		return x.File
	}
	return nil
}

type isConfiguration_Source interface {
	isConfiguration_Source()
}

type Configuration_DevicePath struct {
	DevicePath string `protobuf:"bytes,1,opt,name=device_path,json=devicePath,proto3,oneof"`
}

type Configuration_File struct {
	File *FileConfiguration `protobuf:"bytes,2,opt,name=file,proto3,oneof"`
}

func (*Configuration_DevicePath) isConfiguration_Source() {}

func (*Configuration_File) isConfiguration_Source() {}

var File_pkg_proto_configuration_blockdevice_blockdevice_proto protoreflect.FileDescriptor

var file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDesc = []byte{
	0x0a, 0x35, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x64,
	0x65, 0x76, 0x69, 0x63, 0x65, 0x2f, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x64, 0x65, 0x76, 0x69, 0x63,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x23, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61,
	0x72, 0x6e, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x2e, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x22, 0x46, 0x0a, 0x11,
	0x46, 0x69, 0x6c, 0x65, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x61, 0x74, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x70, 0x61, 0x74, 0x68, 0x12, 0x1d, 0x0a, 0x0a, 0x73, 0x69, 0x7a, 0x65, 0x5f, 0x62, 0x79,
	0x74, 0x65, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x73, 0x69, 0x7a, 0x65, 0x42,
	0x79, 0x74, 0x65, 0x73, 0x22, 0x8a, 0x01, 0x0a, 0x0d, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x21, 0x0a, 0x0b, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65,
	0x5f, 0x70, 0x61, 0x74, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x0a, 0x64,
	0x65, 0x76, 0x69, 0x63, 0x65, 0x50, 0x61, 0x74, 0x68, 0x12, 0x4c, 0x0a, 0x04, 0x66, 0x69, 0x6c,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x36, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62,
	0x61, 0x72, 0x6e, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x2e, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x46, 0x69,
	0x6c, 0x65, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x48,
	0x00, 0x52, 0x04, 0x66, 0x69, 0x6c, 0x65, 0x42, 0x08, 0x0a, 0x06, 0x73, 0x6f, 0x75, 0x72, 0x63,
	0x65, 0x42, 0x45, 0x5a, 0x43, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2f, 0x62, 0x62, 0x2d, 0x73, 0x74, 0x6f,
	0x72, 0x61, 0x67, 0x65, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63,
	0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x62, 0x6c, 0x6f,
	0x63, 0x6b, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDescOnce sync.Once
	file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDescData = file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDesc
)

func file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDescGZIP() []byte {
	file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDescOnce.Do(func() {
		file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDescData)
	})
	return file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDescData
}

var file_pkg_proto_configuration_blockdevice_blockdevice_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_pkg_proto_configuration_blockdevice_blockdevice_proto_goTypes = []any{
	(*FileConfiguration)(nil), // 0: buildbarn.configuration.blockdevice.FileConfiguration
	(*Configuration)(nil),     // 1: buildbarn.configuration.blockdevice.Configuration
}
var file_pkg_proto_configuration_blockdevice_blockdevice_proto_depIdxs = []int32{
	0, // 0: buildbarn.configuration.blockdevice.Configuration.file:type_name -> buildbarn.configuration.blockdevice.FileConfiguration
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_pkg_proto_configuration_blockdevice_blockdevice_proto_init() }
func file_pkg_proto_configuration_blockdevice_blockdevice_proto_init() {
	if File_pkg_proto_configuration_blockdevice_blockdevice_proto != nil {
		return
	}
	file_pkg_proto_configuration_blockdevice_blockdevice_proto_msgTypes[1].OneofWrappers = []any{
		(*Configuration_DevicePath)(nil),
		(*Configuration_File)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_proto_configuration_blockdevice_blockdevice_proto_goTypes,
		DependencyIndexes: file_pkg_proto_configuration_blockdevice_blockdevice_proto_depIdxs,
		MessageInfos:      file_pkg_proto_configuration_blockdevice_blockdevice_proto_msgTypes,
	}.Build()
	File_pkg_proto_configuration_blockdevice_blockdevice_proto = out.File
	file_pkg_proto_configuration_blockdevice_blockdevice_proto_rawDesc = nil
	file_pkg_proto_configuration_blockdevice_blockdevice_proto_goTypes = nil
	file_pkg_proto_configuration_blockdevice_blockdevice_proto_depIdxs = nil
}
