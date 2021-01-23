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
	"sync"
	"time"
)

// Clock is an interface for clocks
type Clock interface {
	// Increment increments the clock
	Increment() Timestamp
}

// NewPhysicalClock creates a new physical clock
func NewPhysicalClock() Clock {
	return &PhysicalClock{}
}

// PhysicalClock is a clock that produces PhysicalTimestamps
type PhysicalClock struct{}

func (c *PhysicalClock) Increment() Timestamp {
	return NewPhysicalTimestamp(PhysicalTime(time.Now()))
}

// NewLogicalClock creates a new logical clock
func NewLogicalClock() Clock {
	return &LogicalClock{}
}

// LogicalClock is a clock that produces LogicalTimestamps
type LogicalClock struct {
	timestamp LogicalTimestamp
	mu        sync.Mutex
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

// NewCompositeClock creates a new composite clock
func NewCompositeClock(clocks ...Clock) Clock {
	return &CompositeClock{
		Clocks: clocks,
	}
}

// CompositeClock is a clock that produces CompositeTimestamps
type CompositeClock struct {
	Clocks []Clock
}

func (c *CompositeClock) Increment() Timestamp {
	timestamps := make([]Timestamp, len(c.Clocks))
	for i, clock := range c.Clocks {
		timestamps[i] = clock.Increment()
	}
	return NewCompositeTimestamp(timestamps...)
}
