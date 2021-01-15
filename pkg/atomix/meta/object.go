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

package meta

import (
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
)

func Equal(m1, m2 metaapi.ObjectMeta) bool {
	return New(m1).Equal(New(m2))
}

// New creates new object metadata from the given proto metadata
func New(meta metaapi.ObjectMeta) ObjectMeta {
	var revision Revision
	if meta.Revision != nil {
		revision = Revision(meta.Revision.Num)
	}
	var timestamp Timestamp
	if meta.Timestamp != nil {
		timestamp = NewTimestamp(*meta.Timestamp)
	}
	return ObjectMeta{
		Revision:  revision,
		Timestamp: timestamp,
	}
}

// Object is an interface for objects
type Object interface {
	// Meta returns the object metadata
	Meta() ObjectMeta
}

// ObjectMeta contains metadata about an object
type ObjectMeta struct {
	Revision  Revision
	Timestamp Timestamp
}

func (m ObjectMeta) Meta() ObjectMeta {
	return m
}

func (m ObjectMeta) Proto() metaapi.ObjectMeta {
	meta := metaapi.ObjectMeta{}
	if m.Revision > 0 {
		meta.Revision = &metaapi.Revision{
			Num: metaapi.RevisionNum(m.Revision),
		}
	}
	if m.Timestamp != nil {
		switch t := m.Timestamp.(type) {
		case PhysicalTimestamp:
			meta.Timestamp = &metaapi.Timestamp{
				Timestamp: &metaapi.Timestamp_PhysicalTimestamp{
					PhysicalTimestamp: &metaapi.PhysicalTimestamp{
						Time: metaapi.PhysicalTime(t.Time),
					},
				},
			}
		case LogicalTimestamp:
			meta.Timestamp = &metaapi.Timestamp{
				Timestamp: &metaapi.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &metaapi.LogicalTimestamp{
						Time: metaapi.LogicalTime(t.Time),
					},
				},
			}
		case VectorTimestamp:
			times := make([]metaapi.LogicalTime, len(t.Times))
			for i, time := range t.Times {
				times[i] = metaapi.LogicalTime(time)
			}
			meta.Timestamp = &metaapi.Timestamp{
				Timestamp: &metaapi.Timestamp_VectorTimestamp{
					VectorTimestamp: &metaapi.VectorTimestamp{
						Time: times,
					},
				},
			}
		case EpochTimestamp:
			meta.Timestamp = &metaapi.Timestamp{
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
	}
	return meta
}

func (m ObjectMeta) Equal(meta ObjectMeta) bool {
	if m.Revision != meta.Revision {
		return false
	}
	if m.Timestamp != nil && meta.Timestamp != nil && !m.Timestamp.Equal(meta.Timestamp) {
		return false
	}
	return true
}

// Revision is a revision number
type Revision uint64
