// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: atomix/storage/protocol/rsm/set/state.proto

package set

import (
	fmt "fmt"
	meta "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type SetState struct {
	Values []SetValue `protobuf:"bytes,1,rep,name=values,proto3" json:"values"`
}

func (m *SetState) Reset()         { *m = SetState{} }
func (m *SetState) String() string { return proto.CompactTextString(m) }
func (*SetState) ProtoMessage()    {}
func (*SetState) Descriptor() ([]byte, []int) {
	return fileDescriptor_f14c30dee47c8ce8, []int{0}
}
func (m *SetState) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *SetState) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_SetState.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *SetState) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SetState.Merge(m, src)
}
func (m *SetState) XXX_Size() int {
	return m.Size()
}
func (m *SetState) XXX_DiscardUnknown() {
	xxx_messageInfo_SetState.DiscardUnknown(m)
}

var xxx_messageInfo_SetState proto.InternalMessageInfo

func (m *SetState) GetValues() []SetValue {
	if m != nil {
		return m.Values
	}
	return nil
}

type SetValue struct {
	meta.ObjectMeta `protobuf:"bytes,1,opt,name=meta,proto3,embedded=meta" json:"meta"`
	Value           string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *SetValue) Reset()         { *m = SetValue{} }
func (m *SetValue) String() string { return proto.CompactTextString(m) }
func (*SetValue) ProtoMessage()    {}
func (*SetValue) Descriptor() ([]byte, []int) {
	return fileDescriptor_f14c30dee47c8ce8, []int{1}
}
func (m *SetValue) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *SetValue) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_SetValue.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *SetValue) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SetValue.Merge(m, src)
}
func (m *SetValue) XXX_Size() int {
	return m.Size()
}
func (m *SetValue) XXX_DiscardUnknown() {
	xxx_messageInfo_SetValue.DiscardUnknown(m)
}

var xxx_messageInfo_SetValue proto.InternalMessageInfo

func (m *SetValue) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

func init() {
	proto.RegisterType((*SetState)(nil), "atomix.storage.protocol.rsm.set.SetState")
	proto.RegisterType((*SetValue)(nil), "atomix.storage.protocol.rsm.set.SetValue")
}

func init() {
	proto.RegisterFile("atomix/storage/protocol/rsm/set/state.proto", fileDescriptor_f14c30dee47c8ce8)
}

var fileDescriptor_f14c30dee47c8ce8 = []byte{
	// 259 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x8e, 0xbd, 0x6a, 0xc3, 0x30,
	0x14, 0x85, 0xad, 0x36, 0x0d, 0xa9, 0xb2, 0x99, 0x0c, 0x26, 0x83, 0xec, 0x7a, 0x72, 0x29, 0x5c,
	0x41, 0xfa, 0x00, 0x05, 0x2f, 0x9d, 0x4a, 0xc1, 0x86, 0xee, 0x4a, 0xb8, 0x18, 0x97, 0x08, 0x05,
	0xe9, 0x36, 0xf4, 0x31, 0xfa, 0x58, 0x19, 0x3d, 0x76, 0x0a, 0xc5, 0x7e, 0x91, 0x22, 0xd9, 0xe9,
	0xda, 0xed, 0xfe, 0x9c, 0xf3, 0x9d, 0xc3, 0x1f, 0x14, 0x19, 0xdd, 0x7e, 0x4a, 0x47, 0xc6, 0xaa,
	0x06, 0xe5, 0xc1, 0x1a, 0x32, 0x3b, 0xb3, 0x97, 0xd6, 0x69, 0xe9, 0x90, 0xa4, 0x23, 0x45, 0x08,
	0xe1, 0x1c, 0xa7, 0xa3, 0x18, 0x26, 0x31, 0x5c, 0xc4, 0x60, 0x9d, 0x06, 0x87, 0xb4, 0xce, 0x27,
	0xda, 0xc1, 0xb6, 0xba, 0xa5, 0xf6, 0x88, 0x52, 0x23, 0x29, 0x69, 0xb6, 0xef, 0xb8, 0xa3, 0x51,
	0xbe, 0x5e, 0x35, 0xa6, 0x31, 0x61, 0x94, 0x7e, 0x1a, 0xaf, 0x79, 0xcd, 0x17, 0x35, 0x52, 0xed,
	0xc3, 0xe2, 0x67, 0x3e, 0x3f, 0xaa, 0xfd, 0x07, 0xba, 0x84, 0x65, 0xd7, 0xc5, 0x72, 0x73, 0x0f,
	0xff, 0xe4, 0x42, 0x8d, 0xf4, 0xe6, 0x1d, 0xe5, 0xec, 0x74, 0x4e, 0xa3, 0x6a, 0xb2, 0xe7, 0x2a,
	0x40, 0xc3, 0x27, 0x7e, 0xe2, 0x33, 0xdf, 0x25, 0x61, 0x19, 0x2b, 0x96, 0x9b, 0xbb, 0x0b, 0xf2,
	0xaf, 0x29, 0xf8, 0x2f, 0xbc, 0x86, 0xa6, 0x2f, 0x48, 0xaa, 0x5c, 0x78, 0x54, 0x77, 0x4e, 0x59,
	0x15, 0x8c, 0xf1, 0x8a, 0xdf, 0x04, 0x6c, 0x72, 0x95, 0xb1, 0xe2, 0xb6, 0x1a, 0x97, 0x32, 0x39,
	0xf5, 0x82, 0x75, 0xbd, 0x60, 0x3f, 0xbd, 0x60, 0x5f, 0x83, 0x88, 0xba, 0x41, 0x44, 0xdf, 0x83,
	0x88, 0xb6, 0xf3, 0xd0, 0xf2, 0xf1, 0x37, 0x00, 0x00, 0xff, 0xff, 0x45, 0xda, 0xda, 0x8c, 0x62,
	0x01, 0x00, 0x00,
}

func (m *SetState) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *SetState) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *SetState) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Values) > 0 {
		for iNdEx := len(m.Values) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Values[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintState(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *SetValue) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *SetValue) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *SetValue) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Value) > 0 {
		i -= len(m.Value)
		copy(dAtA[i:], m.Value)
		i = encodeVarintState(dAtA, i, uint64(len(m.Value)))
		i--
		dAtA[i] = 0x12
	}
	{
		size, err := m.ObjectMeta.MarshalToSizedBuffer(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarintState(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0xa
	return len(dAtA) - i, nil
}

func encodeVarintState(dAtA []byte, offset int, v uint64) int {
	offset -= sovState(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *SetState) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Values) > 0 {
		for _, e := range m.Values {
			l = e.Size()
			n += 1 + l + sovState(uint64(l))
		}
	}
	return n
}

func (m *SetValue) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = m.ObjectMeta.Size()
	n += 1 + l + sovState(uint64(l))
	l = len(m.Value)
	if l > 0 {
		n += 1 + l + sovState(uint64(l))
	}
	return n
}

func sovState(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozState(x uint64) (n int) {
	return sovState(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *SetState) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowState
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: SetState: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: SetState: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Values", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthState
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthState
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Values = append(m.Values, SetValue{})
			if err := m.Values[len(m.Values)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipState(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthState
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthState
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *SetValue) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowState
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: SetValue: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: SetValue: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ObjectMeta", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthState
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthState
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.ObjectMeta.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowState
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthState
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthState
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipState(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthState
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthState
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipState(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowState
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowState
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowState
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthState
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupState
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthState
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthState        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowState          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupState = fmt.Errorf("proto: unexpected end of group")
)