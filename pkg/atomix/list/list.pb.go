// Code generated by protoc-gen-go. DO NOT EDIT.
// source: atomix/list/list.proto

package list

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type ResponseStatus int32

const (
	ResponseStatus_OK            ResponseStatus = 0
	ResponseStatus_NOOP          ResponseStatus = 1
	ResponseStatus_WRITE_LOCK    ResponseStatus = 2
	ResponseStatus_OUT_OF_BOUNDS ResponseStatus = 3
)

var ResponseStatus_name = map[int32]string{
	0: "OK",
	1: "NOOP",
	2: "WRITE_LOCK",
	3: "OUT_OF_BOUNDS",
}

var ResponseStatus_value = map[string]int32{
	"OK":            0,
	"NOOP":          1,
	"WRITE_LOCK":    2,
	"OUT_OF_BOUNDS": 3,
}

func (x ResponseStatus) String() string {
	return proto.EnumName(ResponseStatus_name, int32(x))
}

func (ResponseStatus) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{0}
}

type ListenResponse_Type int32

const (
	ListenResponse_ADDED   ListenResponse_Type = 0
	ListenResponse_REMOVED ListenResponse_Type = 1
)

var ListenResponse_Type_name = map[int32]string{
	0: "ADDED",
	1: "REMOVED",
}

var ListenResponse_Type_value = map[string]int32{
	"ADDED":   0,
	"REMOVED": 1,
}

func (x ListenResponse_Type) String() string {
	return proto.EnumName(ListenResponse_Type_name, int32(x))
}

func (ListenResponse_Type) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{16, 0}
}

// List snapshot
type ListSnapshot struct {
	Values               []string `protobuf:"bytes,1,rep,name=values,proto3" json:"values,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ListSnapshot) Reset()         { *m = ListSnapshot{} }
func (m *ListSnapshot) String() string { return proto.CompactTextString(m) }
func (*ListSnapshot) ProtoMessage()    {}
func (*ListSnapshot) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{0}
}

func (m *ListSnapshot) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ListSnapshot.Unmarshal(m, b)
}
func (m *ListSnapshot) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ListSnapshot.Marshal(b, m, deterministic)
}
func (m *ListSnapshot) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ListSnapshot.Merge(m, src)
}
func (m *ListSnapshot) XXX_Size() int {
	return xxx_messageInfo_ListSnapshot.Size(m)
}
func (m *ListSnapshot) XXX_DiscardUnknown() {
	xxx_messageInfo_ListSnapshot.DiscardUnknown(m)
}

var xxx_messageInfo_ListSnapshot proto.InternalMessageInfo

func (m *ListSnapshot) GetValues() []string {
	if m != nil {
		return m.Values
	}
	return nil
}

type SizeRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SizeRequest) Reset()         { *m = SizeRequest{} }
func (m *SizeRequest) String() string { return proto.CompactTextString(m) }
func (*SizeRequest) ProtoMessage()    {}
func (*SizeRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{1}
}

func (m *SizeRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SizeRequest.Unmarshal(m, b)
}
func (m *SizeRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SizeRequest.Marshal(b, m, deterministic)
}
func (m *SizeRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SizeRequest.Merge(m, src)
}
func (m *SizeRequest) XXX_Size() int {
	return xxx_messageInfo_SizeRequest.Size(m)
}
func (m *SizeRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_SizeRequest.DiscardUnknown(m)
}

var xxx_messageInfo_SizeRequest proto.InternalMessageInfo

type SizeResponse struct {
	Size                 int32    `protobuf:"varint,1,opt,name=size,proto3" json:"size,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SizeResponse) Reset()         { *m = SizeResponse{} }
func (m *SizeResponse) String() string { return proto.CompactTextString(m) }
func (*SizeResponse) ProtoMessage()    {}
func (*SizeResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{2}
}

func (m *SizeResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SizeResponse.Unmarshal(m, b)
}
func (m *SizeResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SizeResponse.Marshal(b, m, deterministic)
}
func (m *SizeResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SizeResponse.Merge(m, src)
}
func (m *SizeResponse) XXX_Size() int {
	return xxx_messageInfo_SizeResponse.Size(m)
}
func (m *SizeResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_SizeResponse.DiscardUnknown(m)
}

var xxx_messageInfo_SizeResponse proto.InternalMessageInfo

func (m *SizeResponse) GetSize() int32 {
	if m != nil {
		return m.Size
	}
	return 0
}

type ContainsRequest struct {
	Value                string   `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ContainsRequest) Reset()         { *m = ContainsRequest{} }
func (m *ContainsRequest) String() string { return proto.CompactTextString(m) }
func (*ContainsRequest) ProtoMessage()    {}
func (*ContainsRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{3}
}

func (m *ContainsRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ContainsRequest.Unmarshal(m, b)
}
func (m *ContainsRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ContainsRequest.Marshal(b, m, deterministic)
}
func (m *ContainsRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ContainsRequest.Merge(m, src)
}
func (m *ContainsRequest) XXX_Size() int {
	return xxx_messageInfo_ContainsRequest.Size(m)
}
func (m *ContainsRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ContainsRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ContainsRequest proto.InternalMessageInfo

func (m *ContainsRequest) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type ContainsResponse struct {
	Contains             bool     `protobuf:"varint,1,opt,name=contains,proto3" json:"contains,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ContainsResponse) Reset()         { *m = ContainsResponse{} }
func (m *ContainsResponse) String() string { return proto.CompactTextString(m) }
func (*ContainsResponse) ProtoMessage()    {}
func (*ContainsResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{4}
}

func (m *ContainsResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ContainsResponse.Unmarshal(m, b)
}
func (m *ContainsResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ContainsResponse.Marshal(b, m, deterministic)
}
func (m *ContainsResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ContainsResponse.Merge(m, src)
}
func (m *ContainsResponse) XXX_Size() int {
	return xxx_messageInfo_ContainsResponse.Size(m)
}
func (m *ContainsResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ContainsResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ContainsResponse proto.InternalMessageInfo

func (m *ContainsResponse) GetContains() bool {
	if m != nil {
		return m.Contains
	}
	return false
}

type AppendRequest struct {
	Value                string   `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AppendRequest) Reset()         { *m = AppendRequest{} }
func (m *AppendRequest) String() string { return proto.CompactTextString(m) }
func (*AppendRequest) ProtoMessage()    {}
func (*AppendRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{5}
}

func (m *AppendRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AppendRequest.Unmarshal(m, b)
}
func (m *AppendRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AppendRequest.Marshal(b, m, deterministic)
}
func (m *AppendRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AppendRequest.Merge(m, src)
}
func (m *AppendRequest) XXX_Size() int {
	return xxx_messageInfo_AppendRequest.Size(m)
}
func (m *AppendRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_AppendRequest.DiscardUnknown(m)
}

var xxx_messageInfo_AppendRequest proto.InternalMessageInfo

func (m *AppendRequest) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type AppendResponse struct {
	Status               ResponseStatus `protobuf:"varint,1,opt,name=status,proto3,enum=atomix.list.service.ResponseStatus" json:"status,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *AppendResponse) Reset()         { *m = AppendResponse{} }
func (m *AppendResponse) String() string { return proto.CompactTextString(m) }
func (*AppendResponse) ProtoMessage()    {}
func (*AppendResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{6}
}

func (m *AppendResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AppendResponse.Unmarshal(m, b)
}
func (m *AppendResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AppendResponse.Marshal(b, m, deterministic)
}
func (m *AppendResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AppendResponse.Merge(m, src)
}
func (m *AppendResponse) XXX_Size() int {
	return xxx_messageInfo_AppendResponse.Size(m)
}
func (m *AppendResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_AppendResponse.DiscardUnknown(m)
}

var xxx_messageInfo_AppendResponse proto.InternalMessageInfo

func (m *AppendResponse) GetStatus() ResponseStatus {
	if m != nil {
		return m.Status
	}
	return ResponseStatus_OK
}

type InsertRequest struct {
	Index                uint32   `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	Value                string   `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *InsertRequest) Reset()         { *m = InsertRequest{} }
func (m *InsertRequest) String() string { return proto.CompactTextString(m) }
func (*InsertRequest) ProtoMessage()    {}
func (*InsertRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{7}
}

func (m *InsertRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_InsertRequest.Unmarshal(m, b)
}
func (m *InsertRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_InsertRequest.Marshal(b, m, deterministic)
}
func (m *InsertRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_InsertRequest.Merge(m, src)
}
func (m *InsertRequest) XXX_Size() int {
	return xxx_messageInfo_InsertRequest.Size(m)
}
func (m *InsertRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_InsertRequest.DiscardUnknown(m)
}

var xxx_messageInfo_InsertRequest proto.InternalMessageInfo

func (m *InsertRequest) GetIndex() uint32 {
	if m != nil {
		return m.Index
	}
	return 0
}

func (m *InsertRequest) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type InsertResponse struct {
	Status               ResponseStatus `protobuf:"varint,1,opt,name=status,proto3,enum=atomix.list.service.ResponseStatus" json:"status,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *InsertResponse) Reset()         { *m = InsertResponse{} }
func (m *InsertResponse) String() string { return proto.CompactTextString(m) }
func (*InsertResponse) ProtoMessage()    {}
func (*InsertResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{8}
}

func (m *InsertResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_InsertResponse.Unmarshal(m, b)
}
func (m *InsertResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_InsertResponse.Marshal(b, m, deterministic)
}
func (m *InsertResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_InsertResponse.Merge(m, src)
}
func (m *InsertResponse) XXX_Size() int {
	return xxx_messageInfo_InsertResponse.Size(m)
}
func (m *InsertResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_InsertResponse.DiscardUnknown(m)
}

var xxx_messageInfo_InsertResponse proto.InternalMessageInfo

func (m *InsertResponse) GetStatus() ResponseStatus {
	if m != nil {
		return m.Status
	}
	return ResponseStatus_OK
}

type GetRequest struct {
	Index                uint32   `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetRequest) Reset()         { *m = GetRequest{} }
func (m *GetRequest) String() string { return proto.CompactTextString(m) }
func (*GetRequest) ProtoMessage()    {}
func (*GetRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{9}
}

func (m *GetRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetRequest.Unmarshal(m, b)
}
func (m *GetRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetRequest.Marshal(b, m, deterministic)
}
func (m *GetRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetRequest.Merge(m, src)
}
func (m *GetRequest) XXX_Size() int {
	return xxx_messageInfo_GetRequest.Size(m)
}
func (m *GetRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetRequest proto.InternalMessageInfo

func (m *GetRequest) GetIndex() uint32 {
	if m != nil {
		return m.Index
	}
	return 0
}

type GetResponse struct {
	Status               ResponseStatus `protobuf:"varint,1,opt,name=status,proto3,enum=atomix.list.service.ResponseStatus" json:"status,omitempty"`
	Value                string         `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *GetResponse) Reset()         { *m = GetResponse{} }
func (m *GetResponse) String() string { return proto.CompactTextString(m) }
func (*GetResponse) ProtoMessage()    {}
func (*GetResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{10}
}

func (m *GetResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetResponse.Unmarshal(m, b)
}
func (m *GetResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetResponse.Marshal(b, m, deterministic)
}
func (m *GetResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetResponse.Merge(m, src)
}
func (m *GetResponse) XXX_Size() int {
	return xxx_messageInfo_GetResponse.Size(m)
}
func (m *GetResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_GetResponse.DiscardUnknown(m)
}

var xxx_messageInfo_GetResponse proto.InternalMessageInfo

func (m *GetResponse) GetStatus() ResponseStatus {
	if m != nil {
		return m.Status
	}
	return ResponseStatus_OK
}

func (m *GetResponse) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type RemoveRequest struct {
	Index                uint32   `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RemoveRequest) Reset()         { *m = RemoveRequest{} }
func (m *RemoveRequest) String() string { return proto.CompactTextString(m) }
func (*RemoveRequest) ProtoMessage()    {}
func (*RemoveRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{11}
}

func (m *RemoveRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RemoveRequest.Unmarshal(m, b)
}
func (m *RemoveRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RemoveRequest.Marshal(b, m, deterministic)
}
func (m *RemoveRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RemoveRequest.Merge(m, src)
}
func (m *RemoveRequest) XXX_Size() int {
	return xxx_messageInfo_RemoveRequest.Size(m)
}
func (m *RemoveRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_RemoveRequest.DiscardUnknown(m)
}

var xxx_messageInfo_RemoveRequest proto.InternalMessageInfo

func (m *RemoveRequest) GetIndex() uint32 {
	if m != nil {
		return m.Index
	}
	return 0
}

type RemoveResponse struct {
	Status               ResponseStatus `protobuf:"varint,1,opt,name=status,proto3,enum=atomix.list.service.ResponseStatus" json:"status,omitempty"`
	Value                string         `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *RemoveResponse) Reset()         { *m = RemoveResponse{} }
func (m *RemoveResponse) String() string { return proto.CompactTextString(m) }
func (*RemoveResponse) ProtoMessage()    {}
func (*RemoveResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{12}
}

func (m *RemoveResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RemoveResponse.Unmarshal(m, b)
}
func (m *RemoveResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RemoveResponse.Marshal(b, m, deterministic)
}
func (m *RemoveResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RemoveResponse.Merge(m, src)
}
func (m *RemoveResponse) XXX_Size() int {
	return xxx_messageInfo_RemoveResponse.Size(m)
}
func (m *RemoveResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_RemoveResponse.DiscardUnknown(m)
}

var xxx_messageInfo_RemoveResponse proto.InternalMessageInfo

func (m *RemoveResponse) GetStatus() ResponseStatus {
	if m != nil {
		return m.Status
	}
	return ResponseStatus_OK
}

func (m *RemoveResponse) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type ClearRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ClearRequest) Reset()         { *m = ClearRequest{} }
func (m *ClearRequest) String() string { return proto.CompactTextString(m) }
func (*ClearRequest) ProtoMessage()    {}
func (*ClearRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{13}
}

func (m *ClearRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ClearRequest.Unmarshal(m, b)
}
func (m *ClearRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ClearRequest.Marshal(b, m, deterministic)
}
func (m *ClearRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ClearRequest.Merge(m, src)
}
func (m *ClearRequest) XXX_Size() int {
	return xxx_messageInfo_ClearRequest.Size(m)
}
func (m *ClearRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ClearRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ClearRequest proto.InternalMessageInfo

type ClearResponse struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ClearResponse) Reset()         { *m = ClearResponse{} }
func (m *ClearResponse) String() string { return proto.CompactTextString(m) }
func (*ClearResponse) ProtoMessage()    {}
func (*ClearResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{14}
}

func (m *ClearResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ClearResponse.Unmarshal(m, b)
}
func (m *ClearResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ClearResponse.Marshal(b, m, deterministic)
}
func (m *ClearResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ClearResponse.Merge(m, src)
}
func (m *ClearResponse) XXX_Size() int {
	return xxx_messageInfo_ClearResponse.Size(m)
}
func (m *ClearResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ClearResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ClearResponse proto.InternalMessageInfo

type ListenRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ListenRequest) Reset()         { *m = ListenRequest{} }
func (m *ListenRequest) String() string { return proto.CompactTextString(m) }
func (*ListenRequest) ProtoMessage()    {}
func (*ListenRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{15}
}

func (m *ListenRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ListenRequest.Unmarshal(m, b)
}
func (m *ListenRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ListenRequest.Marshal(b, m, deterministic)
}
func (m *ListenRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ListenRequest.Merge(m, src)
}
func (m *ListenRequest) XXX_Size() int {
	return xxx_messageInfo_ListenRequest.Size(m)
}
func (m *ListenRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ListenRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ListenRequest proto.InternalMessageInfo

type ListenResponse struct {
	Type                 ListenResponse_Type `protobuf:"varint,1,opt,name=type,proto3,enum=atomix.list.service.ListenResponse_Type" json:"type,omitempty"`
	Value                string              `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{}            `json:"-"`
	XXX_unrecognized     []byte              `json:"-"`
	XXX_sizecache        int32               `json:"-"`
}

func (m *ListenResponse) Reset()         { *m = ListenResponse{} }
func (m *ListenResponse) String() string { return proto.CompactTextString(m) }
func (*ListenResponse) ProtoMessage()    {}
func (*ListenResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{16}
}

func (m *ListenResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ListenResponse.Unmarshal(m, b)
}
func (m *ListenResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ListenResponse.Marshal(b, m, deterministic)
}
func (m *ListenResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ListenResponse.Merge(m, src)
}
func (m *ListenResponse) XXX_Size() int {
	return xxx_messageInfo_ListenResponse.Size(m)
}
func (m *ListenResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ListenResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ListenResponse proto.InternalMessageInfo

func (m *ListenResponse) GetType() ListenResponse_Type {
	if m != nil {
		return m.Type
	}
	return ListenResponse_ADDED
}

func (m *ListenResponse) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type UnlistenRequest struct {
	StreamId             int64    `protobuf:"varint,1,opt,name=stream_id,json=streamId,proto3" json:"stream_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *UnlistenRequest) Reset()         { *m = UnlistenRequest{} }
func (m *UnlistenRequest) String() string { return proto.CompactTextString(m) }
func (*UnlistenRequest) ProtoMessage()    {}
func (*UnlistenRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{17}
}

func (m *UnlistenRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_UnlistenRequest.Unmarshal(m, b)
}
func (m *UnlistenRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_UnlistenRequest.Marshal(b, m, deterministic)
}
func (m *UnlistenRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_UnlistenRequest.Merge(m, src)
}
func (m *UnlistenRequest) XXX_Size() int {
	return xxx_messageInfo_UnlistenRequest.Size(m)
}
func (m *UnlistenRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_UnlistenRequest.DiscardUnknown(m)
}

var xxx_messageInfo_UnlistenRequest proto.InternalMessageInfo

func (m *UnlistenRequest) GetStreamId() int64 {
	if m != nil {
		return m.StreamId
	}
	return 0
}

type UnlistenResponse struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *UnlistenResponse) Reset()         { *m = UnlistenResponse{} }
func (m *UnlistenResponse) String() string { return proto.CompactTextString(m) }
func (*UnlistenResponse) ProtoMessage()    {}
func (*UnlistenResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{18}
}

func (m *UnlistenResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_UnlistenResponse.Unmarshal(m, b)
}
func (m *UnlistenResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_UnlistenResponse.Marshal(b, m, deterministic)
}
func (m *UnlistenResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_UnlistenResponse.Merge(m, src)
}
func (m *UnlistenResponse) XXX_Size() int {
	return xxx_messageInfo_UnlistenResponse.Size(m)
}
func (m *UnlistenResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_UnlistenResponse.DiscardUnknown(m)
}

var xxx_messageInfo_UnlistenResponse proto.InternalMessageInfo

type IterateRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *IterateRequest) Reset()         { *m = IterateRequest{} }
func (m *IterateRequest) String() string { return proto.CompactTextString(m) }
func (*IterateRequest) ProtoMessage()    {}
func (*IterateRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{19}
}

func (m *IterateRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_IterateRequest.Unmarshal(m, b)
}
func (m *IterateRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_IterateRequest.Marshal(b, m, deterministic)
}
func (m *IterateRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_IterateRequest.Merge(m, src)
}
func (m *IterateRequest) XXX_Size() int {
	return xxx_messageInfo_IterateRequest.Size(m)
}
func (m *IterateRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_IterateRequest.DiscardUnknown(m)
}

var xxx_messageInfo_IterateRequest proto.InternalMessageInfo

type IterateResponse struct {
	Value                string   `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *IterateResponse) Reset()         { *m = IterateResponse{} }
func (m *IterateResponse) String() string { return proto.CompactTextString(m) }
func (*IterateResponse) ProtoMessage()    {}
func (*IterateResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f04130b3f66fe801, []int{20}
}

func (m *IterateResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_IterateResponse.Unmarshal(m, b)
}
func (m *IterateResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_IterateResponse.Marshal(b, m, deterministic)
}
func (m *IterateResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_IterateResponse.Merge(m, src)
}
func (m *IterateResponse) XXX_Size() int {
	return xxx_messageInfo_IterateResponse.Size(m)
}
func (m *IterateResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_IterateResponse.DiscardUnknown(m)
}

var xxx_messageInfo_IterateResponse proto.InternalMessageInfo

func (m *IterateResponse) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

func init() {
	proto.RegisterEnum("atomix.list.service.ResponseStatus", ResponseStatus_name, ResponseStatus_value)
	proto.RegisterEnum("atomix.list.service.ListenResponse_Type", ListenResponse_Type_name, ListenResponse_Type_value)
	proto.RegisterType((*ListSnapshot)(nil), "atomix.list.service.ListSnapshot")
	proto.RegisterType((*SizeRequest)(nil), "atomix.list.service.SizeRequest")
	proto.RegisterType((*SizeResponse)(nil), "atomix.list.service.SizeResponse")
	proto.RegisterType((*ContainsRequest)(nil), "atomix.list.service.ContainsRequest")
	proto.RegisterType((*ContainsResponse)(nil), "atomix.list.service.ContainsResponse")
	proto.RegisterType((*AppendRequest)(nil), "atomix.list.service.AppendRequest")
	proto.RegisterType((*AppendResponse)(nil), "atomix.list.service.AppendResponse")
	proto.RegisterType((*InsertRequest)(nil), "atomix.list.service.InsertRequest")
	proto.RegisterType((*InsertResponse)(nil), "atomix.list.service.InsertResponse")
	proto.RegisterType((*GetRequest)(nil), "atomix.list.service.GetRequest")
	proto.RegisterType((*GetResponse)(nil), "atomix.list.service.GetResponse")
	proto.RegisterType((*RemoveRequest)(nil), "atomix.list.service.RemoveRequest")
	proto.RegisterType((*RemoveResponse)(nil), "atomix.list.service.RemoveResponse")
	proto.RegisterType((*ClearRequest)(nil), "atomix.list.service.ClearRequest")
	proto.RegisterType((*ClearResponse)(nil), "atomix.list.service.ClearResponse")
	proto.RegisterType((*ListenRequest)(nil), "atomix.list.service.ListenRequest")
	proto.RegisterType((*ListenResponse)(nil), "atomix.list.service.ListenResponse")
	proto.RegisterType((*UnlistenRequest)(nil), "atomix.list.service.UnlistenRequest")
	proto.RegisterType((*UnlistenResponse)(nil), "atomix.list.service.UnlistenResponse")
	proto.RegisterType((*IterateRequest)(nil), "atomix.list.service.IterateRequest")
	proto.RegisterType((*IterateResponse)(nil), "atomix.list.service.IterateResponse")
}

func init() { proto.RegisterFile("atomix/list/list.proto", fileDescriptor_f04130b3f66fe801) }

var fileDescriptor_f04130b3f66fe801 = []byte{
	// 475 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xb4, 0x94, 0xdd, 0x8f, 0xd2, 0x40,
	0x14, 0xc5, 0xb7, 0x7c, 0x09, 0x07, 0x5a, 0xea, 0x68, 0x36, 0x1b, 0x4d, 0xcc, 0x66, 0xcc, 0xba,
	0xc4, 0x87, 0x9a, 0xe8, 0xe3, 0xfa, 0xb2, 0x42, 0x35, 0x64, 0x3f, 0x6a, 0x06, 0xd0, 0x47, 0xac,
	0x70, 0x13, 0x9b, 0x40, 0x5b, 0x3b, 0x03, 0xd9, 0xdd, 0x67, 0xff, 0x70, 0xc3, 0x74, 0x0a, 0x68,
	0x58, 0x9e, 0x76, 0x5f, 0x9a, 0xde, 0x93, 0x73, 0xcf, 0xfc, 0xee, 0x74, 0xa6, 0x38, 0x0c, 0x55,
	0x32, 0x8f, 0x6e, 0xde, 0xcd, 0x22, 0xa9, 0xf4, 0xc3, 0x4b, 0xb3, 0x44, 0x25, 0xec, 0x59, 0xae,
	0x7b, 0x5a, 0x92, 0x94, 0x2d, 0xa3, 0x09, 0xf1, 0x37, 0x68, 0x5d, 0x46, 0x52, 0x0d, 0xe2, 0x30,
	0x95, 0xbf, 0x12, 0xc5, 0x0e, 0x51, 0x5b, 0x86, 0xb3, 0x05, 0xc9, 0x23, 0xeb, 0xb8, 0xdc, 0x69,
	0x08, 0x53, 0x71, 0x1b, 0xcd, 0x41, 0x74, 0x47, 0x82, 0x7e, 0x2f, 0x48, 0x2a, 0xce, 0xd1, 0xca,
	0x4b, 0x99, 0x26, 0xb1, 0x24, 0xc6, 0x50, 0x91, 0xd1, 0x1d, 0x1d, 0x59, 0xc7, 0x56, 0xa7, 0x2a,
	0xf4, 0x3b, 0x3f, 0x45, 0xbb, 0x9b, 0xc4, 0x2a, 0x8c, 0x62, 0x69, 0xda, 0xd8, 0x73, 0x54, 0x75,
	0x9e, 0xf6, 0x35, 0x44, 0x5e, 0x70, 0x0f, 0xee, 0xc6, 0x68, 0x02, 0x5f, 0xa0, 0x3e, 0x31, 0x9a,
	0x36, 0xd7, 0xc5, 0xba, 0xe6, 0x27, 0xb0, 0xcf, 0xd3, 0x94, 0xe2, 0xe9, 0xfe, 0xd8, 0x2b, 0x38,
	0x85, 0xcd, 0x84, 0x9e, 0xa1, 0x26, 0x55, 0xa8, 0x16, 0x79, 0xa4, 0xf3, 0xfe, 0xb5, 0xb7, 0x63,
	0x4b, 0xbc, 0xc2, 0x3e, 0xd0, 0x56, 0x61, 0x5a, 0xf8, 0x19, 0xec, 0x7e, 0x2c, 0x29, 0x53, 0x5b,
	0xab, 0x46, 0xf1, 0x94, 0x6e, 0x74, 0x98, 0x2d, 0xf2, 0x62, 0xc3, 0x52, 0xfa, 0x8f, 0xa5, 0x68,
	0x7e, 0x08, 0x16, 0x0e, 0x7c, 0xa1, 0xfd, 0x20, 0xfc, 0x07, 0x9a, 0xda, 0xf3, 0x00, 0xeb, 0xdd,
	0x33, 0xd4, 0x09, 0x6c, 0x41, 0xf3, 0x64, 0x49, 0xfb, 0x41, 0x26, 0x70, 0x0a, 0xdb, 0xe3, 0xb1,
	0x38, 0x68, 0x75, 0x67, 0x14, 0x66, 0xc5, 0x01, 0x6d, 0xc3, 0x36, 0x75, 0x1e, 0xb2, 0x12, 0x56,
	0x07, 0x9d, 0xe2, 0xc2, 0xf1, 0xc7, 0x82, 0x53, 0x28, 0x86, 0xeb, 0x23, 0x2a, 0xea, 0x36, 0x25,
	0x43, 0xd5, 0xd9, 0x49, 0xf5, 0x6f, 0x8b, 0x37, 0xbc, 0x4d, 0x49, 0xe8, 0xae, 0x7b, 0xc0, 0x5e,
	0xa1, 0xb2, 0xf2, 0xb0, 0x06, 0xaa, 0xe7, 0xbd, 0x9e, 0xdf, 0x73, 0x0f, 0x58, 0x13, 0x4f, 0x84,
	0x7f, 0x15, 0x7c, 0xf3, 0x7b, 0xae, 0xc5, 0x3d, 0xb4, 0x47, 0xf1, 0x6c, 0x9b, 0x8c, 0xbd, 0x44,
	0x43, 0xaa, 0x8c, 0xc2, 0xf9, 0x38, 0x9a, 0x6a, 0x96, 0xb2, 0xa8, 0xe7, 0x42, 0x7f, 0xca, 0x19,
	0xdc, 0x8d, 0xdf, 0xcc, 0xe6, 0xc2, 0xe9, 0x2b, 0xca, 0x42, 0xb5, 0xbe, 0x9f, 0xa7, 0x68, 0xaf,
	0x15, 0x33, 0xdc, 0xce, 0x4b, 0xf2, 0xd6, 0x5f, 0x7d, 0x9c, 0xed, 0x7d, 0x66, 0x35, 0x94, 0x82,
	0x0b, 0xf7, 0x80, 0xd5, 0x51, 0xb9, 0x0e, 0x82, 0xaf, 0xae, 0xc5, 0x1c, 0xe0, 0xbb, 0xe8, 0x0f,
	0xfd, 0xf1, 0x65, 0xd0, 0xbd, 0x70, 0x4b, 0xec, 0x29, 0xec, 0x60, 0x34, 0x1c, 0x07, 0x9f, 0xc7,
	0x9f, 0x82, 0xd1, 0x75, 0x6f, 0xe0, 0x96, 0x7f, 0xd6, 0xf4, 0x2f, 0xe6, 0xc3, 0xdf, 0x00, 0x00,
	0x00, 0xff, 0xff, 0x9c, 0xc5, 0xa9, 0x83, 0x7c, 0x04, 0x00, 0x00,
}