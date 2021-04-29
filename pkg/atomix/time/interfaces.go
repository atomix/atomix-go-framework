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

import metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"

// NewTimestamp creates new object timestamp from the given proto timestamp
func NewTimestamp(meta metaapi.Timestamp) Timestamp {
	switch t := meta.Timestamp.(type) {
	case *metaapi.Timestamp_PhysicalTimestamp:
		return NewPhysicalTimestamp(PhysicalTime(t.PhysicalTimestamp.Time))
	case *metaapi.Timestamp_LogicalTimestamp:
		return NewLogicalTimestamp(LogicalTime(t.LogicalTimestamp.Time))
	case *metaapi.Timestamp_EpochTimestamp:
		return NewEpochTimestamp(Epoch(t.EpochTimestamp.Epoch.Num), LogicalTime(t.EpochTimestamp.Sequence.Num))
	case *metaapi.Timestamp_CompositeTimestamp:
		timestamps := make([]Timestamp, 0, len(t.CompositeTimestamp.Timestamps))
		for _, timestamp := range t.CompositeTimestamp.Timestamps {
			timestamps = append(timestamps, NewTimestamp(timestamp))
		}
		return NewCompositeTimestamp(timestamps...)
	default:
		panic("unknown timestamp type")
	}
}

// Scheme it a time scheme
type Scheme interface {
	// Name returns the scheme's name
	Name() string

	// Codec returns the scheme's codec
	Codec() Codec

	// NewClock creates a new clock
	NewClock() Clock
}

// Clock is an interface for clocks
type Clock interface {
	// Scheme returns the clock's scheme
	Scheme() Scheme
	// Get gets the current timestamp
	Get() Timestamp
	// Increment increments the clock
	Increment() Timestamp
	// Update updates the timestamp
	Update(Timestamp) Timestamp
}

// Codec is a time codec
type Codec interface {
	EncodeTimestamp(Timestamp) metaapi.Timestamp
	DecodeTimestamp(metaapi.Timestamp) (Timestamp, error)
}

// Timestamp is a timestamp
type Timestamp interface {
	Scheme() Scheme
	Before(Timestamp) bool
	After(Timestamp) bool
	Equal(Timestamp) bool
}
