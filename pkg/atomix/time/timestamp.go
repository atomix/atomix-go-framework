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
	"time"
)

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

// Timestamp is a timestamp
type Timestamp interface {
	Scheme() Scheme
	Before(Timestamp) bool
	After(Timestamp) bool
	Equal(Timestamp) bool
}

type LogicalTime uint64

func NewLogicalTimestamp(time LogicalTime) Timestamp {
	return LogicalTimestamp{
		Time: time,
	}
}

type LogicalTimestamp struct {
	Time LogicalTime
}

func (t LogicalTimestamp) Scheme() Scheme {
	return LogicalScheme
}

func (t LogicalTimestamp) Increment() LogicalTimestamp {
	return LogicalTimestamp{
		Time: t.Time + 1,
	}
}

func (t LogicalTimestamp) Before(u Timestamp) bool {
	v, ok := u.(LogicalTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}
	return t.Time < v.Time
}

func (t LogicalTimestamp) After(u Timestamp) bool {
	v, ok := u.(LogicalTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}
	return t.Time > v.Time
}

func (t LogicalTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(LogicalTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}
	return t.Time == v.Time
}

type PhysicalTime time.Time

func NewPhysicalTimestamp(time PhysicalTime) Timestamp {
	return PhysicalTimestamp{
		Time: time,
	}
}

type PhysicalTimestamp struct {
	Time PhysicalTime
}

func (t PhysicalTimestamp) Scheme() Scheme {
	return PhysicalScheme
}

func (t PhysicalTimestamp) Before(u Timestamp) bool {
	v, ok := u.(PhysicalTimestamp)
	if !ok {
		panic("not a wall clock timestamp")
	}
	return time.Time(t.Time).Before(time.Time(v.Time))
}

func (t PhysicalTimestamp) After(u Timestamp) bool {
	v, ok := u.(PhysicalTimestamp)
	if !ok {
		panic("not a wall clock timestamp")
	}
	return time.Time(t.Time).After(time.Time(v.Time))
}

func (t PhysicalTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(PhysicalTimestamp)
	if !ok {
		panic("not a wall clock timestamp")
	}
	return time.Time(t.Time).Equal(time.Time(v.Time))
}

type Epoch uint64

func NewEpochTimestamp(epoch Epoch, time LogicalTime) Timestamp {
	return EpochTimestamp{
		Epoch: epoch,
		Time:  time,
	}
}

type EpochTimestamp struct {
	Epoch Epoch
	Time  LogicalTime
}

func (t EpochTimestamp) Scheme() Scheme {
	return EpochScheme
}

func (t EpochTimestamp) Before(u Timestamp) bool {
	v, ok := u.(EpochTimestamp)
	if !ok {
		panic("not an epoch timestamp")
	}
	return t.Epoch < v.Epoch || (t.Epoch == v.Epoch && t.Time < v.Time)
}

func (t EpochTimestamp) After(u Timestamp) bool {
	v, ok := u.(EpochTimestamp)
	if !ok {
		panic("not an epoch timestamp")
	}
	return t.Epoch > v.Epoch || (t.Epoch == v.Epoch && t.Time > v.Time)
}

func (t EpochTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(EpochTimestamp)
	if !ok {
		panic("not an epoch timestamp")
	}
	return t.Epoch == v.Epoch && t.Time == v.Time
}

func NewCompositeTimestamp(timestamps ...Timestamp) Timestamp {
	schemes := make([]Scheme, len(timestamps))
	for i, timestamp := range timestamps {
		schemes[i] = timestamp.Scheme()
	}
	return CompositeTimestamp{
		scheme:     newCompositeScheme(schemes...),
		Timestamps: timestamps,
	}
}

type CompositeTimestamp struct {
	scheme     Scheme
	Timestamps []Timestamp
}

func (t CompositeTimestamp) Scheme() Scheme {
	return t.scheme
}

func (t CompositeTimestamp) Before(u Timestamp) bool {
	v, ok := u.(CompositeTimestamp)
	if !ok {
		panic("not a composite timestamp")
	}
	if len(t.Timestamps) != len(v.Timestamps) {
		panic("incompatible composite timestamps")
	}
	for i := 0; i < len(t.Timestamps); i++ {
		t1 := t.Timestamps[i]
		t2 := v.Timestamps[i]
		if t1.Before(t2) {
			return true
		} else if i > 0 {
			for j := 0; j < i; j++ {
				v1 := t.Timestamps[j]
				v2 := v.Timestamps[j]
				if !v1.Equal(v2) {
					return false
				}
			}
			if !t1.Before(t2) {
				return false
			}
		}
	}
	return true
}

func (t CompositeTimestamp) After(u Timestamp) bool {
	v, ok := u.(CompositeTimestamp)
	if !ok {
		panic("not a composite timestamp")
	}
	if len(t.Timestamps) != len(v.Timestamps) {
		panic("incompatible composite timestamps")
	}
	for i := 0; i < len(t.Timestamps); i++ {
		t1 := t.Timestamps[i]
		t2 := v.Timestamps[i]
		if t1.After(t2) {
			return true
		} else if i > 0 {
			for j := 0; j < i; j++ {
				v1 := t.Timestamps[j]
				v2 := v.Timestamps[j]
				if !v1.Equal(v2) {
					return false
				}
			}
			if !t1.After(t2) {
				return false
			}
		}
	}
	return true
}

func (t CompositeTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(CompositeTimestamp)
	if !ok {
		panic("not a composite timestamp")
	}
	if len(t.Timestamps) != len(v.Timestamps) {
		panic("incompatible composite timestamps")
	}
	for i := 0; i < len(t.Timestamps); i++ {
		t1 := t.Timestamps[i]
		t2 := v.Timestamps[i]
		if !t1.Equal(t2) {
			return false
		}
	}
	return true
}
