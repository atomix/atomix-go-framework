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
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"sync"
)

const epochSchemeName = "Epoch"

var EpochScheme = newEpochScheme()

// newEpochScheme creates a new Epoch scheme
func newEpochScheme() Scheme {
	return epochScheme{
		codec: EpochTimestampCodec{},
	}
}

type epochScheme struct {
	codec Codec
}

func (s epochScheme) Name() string {
	return epochSchemeName
}

func (s epochScheme) Codec() Codec {
	return s.codec
}

func (s epochScheme) NewClock() Clock {
	return NewEpochClock()
}

// NewEpochClock creates a new epoch clock
func NewEpochClock() Clock {
	return &EpochClock{
		timestamp: NewEpochTimestamp(Epoch(0), LogicalTime(0)).(EpochTimestamp),
	}
}

// EpochClock is a clock that produces EpochTimestamps
type EpochClock struct {
	timestamp EpochTimestamp
	mu        sync.RWMutex
}

func (c *EpochClock) Scheme() Scheme {
	return EpochScheme
}

func (c *EpochClock) Get() Timestamp {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp
}

func (c *EpochClock) Increment() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timestamp = NewEpochTimestamp(c.timestamp.Epoch, c.timestamp.Time+1).(EpochTimestamp)
	return c.timestamp
}

func (c *EpochClock) Update(t Timestamp) Timestamp {
	update, ok := t.(EpochTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}

	c.mu.RLock()
	current := c.timestamp
	c.mu.RUnlock()
	if !update.After(current) {
		return current
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if update.After(c.timestamp) {
		c.timestamp = update
	}
	return c.timestamp
}

type Epoch uint64

// NewEpochTimestamp creates a new Epoch based Timestamp
func NewEpochTimestamp(epoch Epoch, time LogicalTime) Timestamp {
	return EpochTimestamp{
		Epoch: epoch,
		Time:  time,
	}
}

// EpochTimestamp is a Timestamp based on an Epoch and LogicalTime
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

// EpochTimestampCodec is a codec for epoch timestamps
type EpochTimestampCodec struct{}

func (c EpochTimestampCodec) EncodeTimestamp(timestamp Timestamp) metaapi.Timestamp {
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

func (c EpochTimestampCodec) DecodeTimestamp(timestamp metaapi.Timestamp) (Timestamp, error) {
	return NewEpochTimestamp(Epoch(timestamp.GetEpochTimestamp().Epoch.Num), LogicalTime(timestamp.GetEpochTimestamp().Sequence.Num)), nil
}
