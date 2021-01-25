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

const physicalSchemeName = "Physical"
const logicalSchemeName = "Logical"
const epochSchemeName = "Epoch"
const compositeSchemeName = "Composite"

// Scheme it a time scheme
type Scheme interface {
	// Name returns the scheme's name
	Name() string

	// TimestampCodec returns the scheme's codec
	Codec() TimestampCodec

	// NewClock creates a new clock
	NewClock() Clock
}

var PhysicalScheme = newPhysicalScheme()

// newPhysicalScheme creates a new physical scheme
func newPhysicalScheme() Scheme {
	return physicalScheme{
		codec: PhysicalTimestampCodec{},
	}
}

type physicalScheme struct {
	codec TimestampCodec
}

func (s physicalScheme) Name() string {
	return physicalSchemeName
}

func (s physicalScheme) Codec() TimestampCodec {
	return s.codec
}

func (s physicalScheme) NewClock() Clock {
	return NewPhysicalClock()
}

var LogicalScheme = newLogicalScheme()

// newLogicalScheme creates a new logical scheme
func newLogicalScheme() Scheme {
	return logicalScheme{
		codec: LogicalTimestampCodec{},
	}
}

type logicalScheme struct {
	codec TimestampCodec
}

func (s logicalScheme) Name() string {
	return logicalSchemeName
}

func (s logicalScheme) Codec() TimestampCodec {
	return s.codec
}

func (s logicalScheme) NewClock() Clock {
	return NewLogicalClock()
}

var EpochScheme = newEpochScheme()

// newEpochScheme creates a new Epoch scheme
func newEpochScheme() Scheme {
	return epochScheme{
		codec: EpochTimestampCodec{},
	}
}

type epochScheme struct {
	codec TimestampCodec
}

func (s epochScheme) Name() string {
	return epochSchemeName
}

func (s epochScheme) Codec() TimestampCodec {
	return s.codec
}

func (s epochScheme) NewClock() Clock {
	return NewEpochClock()
}

// newCompositeScheme creates a new composite scheme
func newCompositeScheme(schemes ...Scheme) Scheme {
	codecs := make([]TimestampCodec, len(schemes))
	for i, scheme := range schemes {
		codecs[i] = scheme.Codec()
	}
	return compositeScheme{
		schemes: schemes,
		codec:   CompositeTimestampCodec{codecs},
	}
}

type compositeScheme struct {
	schemes []Scheme
	codec   TimestampCodec
}

func (s compositeScheme) Name() string {
	return compositeSchemeName
}

func (s compositeScheme) Codec() TimestampCodec {
	return s.codec
}

func (s compositeScheme) NewClock() Clock {
	return NewCompositeClock(s.schemes...)
}
