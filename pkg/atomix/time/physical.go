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
	"time"
)

const physicalSchemeName = "Physical"

var PhysicalScheme = newPhysicalScheme()

// newPhysicalScheme creates a new physical scheme
func newPhysicalScheme() Scheme {
	return physicalScheme{
		codec: PhysicalTimestampCodec{},
	}
}

type physicalScheme struct {
	codec Codec
}

func (s physicalScheme) Name() string {
	return physicalSchemeName
}

func (s physicalScheme) Codec() Codec {
	return s.codec
}

func (s physicalScheme) NewClock() Clock {
	return NewPhysicalClock()
}

// NewPhysicalClock creates a new physical clock
func NewPhysicalClock() Clock {
	return &PhysicalClock{
		timestamp: NewPhysicalTimestamp(PhysicalTime(time.Now())),
	}
}

// PhysicalClock is a clock that produces PhysicalTimestamps
type PhysicalClock struct {
	timestamp Timestamp
	mu        sync.RWMutex
}

func (c *PhysicalClock) Scheme() Scheme {
	return PhysicalScheme
}

func (c *PhysicalClock) Get() Timestamp {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp
}

func (c *PhysicalClock) Increment() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timestamp = NewPhysicalTimestamp(PhysicalTime(time.Now()))
	return c.timestamp
}

func (c *PhysicalClock) Update(update Timestamp) Timestamp {
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

// PhysicalTime is an instant in physical time
type PhysicalTime time.Time

// NewPhysicalTimestamp creates a new Timestamp based in PhysicalTime
func NewPhysicalTimestamp(time PhysicalTime) Timestamp {
	return PhysicalTimestamp{
		Time: time,
	}
}

// PhysicalTimestamp is a Timestamp based on PhysicalTime
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

// PhysicalTimestampCodec is a codec for physical timestamps
type PhysicalTimestampCodec struct{}

func (c PhysicalTimestampCodec) EncodeTimestamp(timestamp Timestamp) metaapi.Timestamp {
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

func (c PhysicalTimestampCodec) DecodeTimestamp(timestamp metaapi.Timestamp) (Timestamp, error) {
	return NewPhysicalTimestamp(PhysicalTime(timestamp.GetPhysicalTimestamp().Time)), nil
}
