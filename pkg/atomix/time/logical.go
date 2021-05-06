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

const logicalSchemeName = "Logical"

// LogicalScheme is a default Scheme for logical time
var LogicalScheme = newLogicalScheme()

// newLogicalScheme creates a new logical scheme
func newLogicalScheme() Scheme {
	return logicalScheme{
		codec: LogicalTimestampCodec{},
	}
}

type logicalScheme struct {
	codec Codec
}

func (s logicalScheme) Name() string {
	return logicalSchemeName
}

func (s logicalScheme) Codec() Codec {
	return s.codec
}

func (s logicalScheme) NewClock() Clock {
	return NewLogicalClock()
}

// NewLogicalClock creates a new logical clock
func NewLogicalClock() Clock {
	return &LogicalClock{
		timestamp: NewLogicalTimestamp(LogicalTime(0)).(LogicalTimestamp),
	}
}

// LogicalClock is a clock that produces LogicalTimestamps
type LogicalClock struct {
	timestamp LogicalTimestamp
	mu        sync.RWMutex
}

func (c *LogicalClock) Scheme() Scheme {
	return LogicalScheme
}

func (c *LogicalClock) Get() Timestamp {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp
}

func (c *LogicalClock) Increment() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	timestamp := LogicalTimestamp{
		Time: c.timestamp.Time + 1,
	}
	c.timestamp = timestamp
	return timestamp
}

func (c *LogicalClock) Update(t Timestamp) Timestamp {
	update, ok := t.(LogicalTimestamp)
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

// LogicalTime is an instant in logical time
type LogicalTime uint64

// NewLogicalTimestamp creates a new logical Timestamp
func NewLogicalTimestamp(time LogicalTime) Timestamp {
	return LogicalTimestamp{
		Time: time,
	}
}

// LogicalTimestamp is a logical timestamp
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

// LogicalTimestampCodec is a codec for logical timestamps
type LogicalTimestampCodec struct{}

func (c LogicalTimestampCodec) EncodeTimestamp(timestamp Timestamp) metaapi.Timestamp {
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

func (c LogicalTimestampCodec) DecodeTimestamp(timestamp metaapi.Timestamp) (Timestamp, error) {
	return NewLogicalTimestamp(LogicalTime(timestamp.GetLogicalTimestamp().Time)), nil
}
