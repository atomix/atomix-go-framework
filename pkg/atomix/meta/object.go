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
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
)

func Equal(m1, m2 metaapi.ObjectMeta) bool {
	return FromProto(m1).Equal(FromProto(m2))
}

// NewRevision creates a new object metadata with the given revision
func NewRevision(revision Revision) ObjectMeta {
	return ObjectMeta{
		Revision: revision,
	}
}

// NewTimestamped creates a new object metadata with the given timestamp
func NewTimestamped(timestamp time.Timestamp) ObjectMeta {
	return ObjectMeta{
		Timestamp: timestamp,
	}
}

// FromProto creates new object metadata from the given proto metadata
func FromProto(meta metaapi.ObjectMeta) ObjectMeta {
	var revision Revision
	if meta.Revision != nil {
		revision = Revision(meta.Revision.Num)
	}
	var timestamp time.Timestamp
	if meta.Timestamp != nil {
		timestamp = time.NewTimestamp(*meta.Timestamp)
	}
	return ObjectMeta{
		Revision:  revision,
		Timestamp: timestamp,
		Tombstone: meta.Type == metaapi.ObjectMeta_TOMBSTONE,
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
	Timestamp time.Timestamp
	Tombstone bool
}

// AsObject returns the metadata as a non-tombstone
func (m ObjectMeta) AsObject() ObjectMeta {
	copy := m
	copy.Tombstone = false
	return copy
}

// AsTombstone returns the metadata as a tombstone
func (m ObjectMeta) AsTombstone() ObjectMeta {
	copy := m
	copy.Tombstone = true
	return copy
}

// Meta implements the Object interface
func (m ObjectMeta) Meta() ObjectMeta {
	return m
}

// Proto returns the metadata in Protobuf format
func (m ObjectMeta) Proto() metaapi.ObjectMeta {
	meta := metaapi.ObjectMeta{}
	if m.Revision > 0 {
		meta.Revision = &metaapi.Revision{
			Num: metaapi.RevisionNum(m.Revision),
		}
	}
	if m.Timestamp != nil {
		timestamp := m.Timestamp.Scheme().Codec().EncodeTimestamp(m.Timestamp)
		meta.Timestamp = &timestamp
	}
	if m.Tombstone {
		meta.Type = metaapi.ObjectMeta_TOMBSTONE
	} else {
		meta.Type = metaapi.ObjectMeta_OBJECT
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

func (m ObjectMeta) Before(meta ObjectMeta) bool {
	if m.Revision != 0 && meta.Revision != 0 && m.Revision >= meta.Revision {
		return false
	}
	if m.Timestamp != nil && meta.Timestamp != nil && !m.Timestamp.Before(meta.Timestamp) {
		return false
	}
	return true
}

func (m ObjectMeta) After(meta ObjectMeta) bool {
	if m.Revision != 0 && meta.Revision != 0 && m.Revision <= meta.Revision {
		return false
	}
	if m.Timestamp != nil && meta.Timestamp != nil && !m.Timestamp.After(meta.Timestamp) {
		return false
	}
	return true
}

// Revision is a revision number
type Revision uint64
