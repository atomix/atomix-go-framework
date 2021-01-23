// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package time

import (
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
)

// TimestampCodec is a timestamp codec
type TimestampCodec interface {
	EncodeProto(Timestamp) metaapi.Timestamp
	DecodeProto(metaapi.Timestamp) (Timestamp, error)
}

// PhysicalTimestampCodec is a codec for physical timestamps
type PhysicalTimestampCodec struct{}

func (c PhysicalTimestampCodec) EncodeProto(timestamp Timestamp) metaapi.Timestamp {
	t, ok := timestamp.(PhysicalTimestamp)
	if !ok {
		panic("expected PhysicalTimestamp")
	}
	return metaapi.Timestamp{
		Timestamp: &metaapi.Timestamp_PhysicalTimestamp{
			PhysicalTimestamp: &metaapi.PhysicalTimestamp{
				Time: metaapi.PhysicalTime(t.Time),
			},
		},
	}
}

func (c PhysicalTimestampCodec) DecodeProto(timestamp metaapi.Timestamp) (Timestamp, error) {
	return NewPhysicalTimestamp(PhysicalTime(timestamp.GetPhysicalTimestamp().Time)), nil
}

// LogicalTimestampCodec is a codec for logical timestamps
type LogicalTimestampCodec struct{}

func (c LogicalTimestampCodec) EncodeProto(timestamp Timestamp) metaapi.Timestamp {
	t, ok := timestamp.(LogicalTimestamp)
	if !ok {
		panic("expected LogicalTimestamp")
	}
	return metaapi.Timestamp{
		Timestamp: &metaapi.Timestamp_LogicalTimestamp{
			LogicalTimestamp: &metaapi.LogicalTimestamp{
				Time: metaapi.LogicalTime(t.Time),
			},
		},
	}
}

func (c LogicalTimestampCodec) DecodeProto(timestamp metaapi.Timestamp) (Timestamp, error) {
	return NewLogicalTimestamp(LogicalTime(timestamp.GetLogicalTimestamp().Time)), nil
}

// EpochTimestampCodec is a codec for epoch timestamps
type EpochTimestampCodec struct{}

func (c EpochTimestampCodec) EncodeProto(timestamp Timestamp) metaapi.Timestamp {
	t, ok := timestamp.(EpochTimestamp)
	if !ok {
		panic("expected EpochTimestamp")
	}
	return metaapi.Timestamp{
		Timestamp: &metaapi.Timestamp_EpochTimestamp{
			EpochTimestamp: &metaapi.EpochTimestamp{
				Epoch: metaapi.Epoch{
					Num: metaapi.EpochNum(t.Epoch),
				},
				Sequence: metaapi.Sequence{
					Num: metaapi.SequenceNum(t.Time),
				},
			},
		},
	}
}

func (c EpochTimestampCodec) DecodeProto(timestamp metaapi.Timestamp) (Timestamp, error) {
	return NewEpochTimestamp(Epoch(timestamp.GetEpochTimestamp().Epoch.Num), LogicalTime(timestamp.GetEpochTimestamp().Sequence.Num)), nil
}

// CompositeTimestampCodec is a codec for Composite timestamps
type CompositeTimestampCodec struct {
	codecs []TimestampCodec
}

func (c CompositeTimestampCodec) EncodeProto(timestamp Timestamp) metaapi.Timestamp {
	t, ok := timestamp.(CompositeTimestamp)
	if !ok {
		panic("expected CompositeTimestamp")
	}
	timestamps := make([]metaapi.Timestamp, 0, len(t.Timestamps))
	for _, timestamp := range t.Timestamps {
		timestamps = append(timestamps, timestamp.Scheme().Codec().EncodeProto(timestamp))
	}
	return metaapi.Timestamp{
		Timestamp: &metaapi.Timestamp_CompositeTimestamp{
			CompositeTimestamp: &metaapi.CompositeTimestamp{
				Timestamps: timestamps,
			},
		},
	}
}

func (c CompositeTimestampCodec) DecodeProto(timestamp metaapi.Timestamp) (Timestamp, error) {
	timestamps := make([]Timestamp, 0, len(timestamp.GetCompositeTimestamp().Timestamps))
	for _, timestamp := range timestamp.GetCompositeTimestamp().Timestamps {
		timestamps = append(timestamps, NewTimestamp(timestamp))
	}
	return NewCompositeTimestamp(timestamps...), nil
}
