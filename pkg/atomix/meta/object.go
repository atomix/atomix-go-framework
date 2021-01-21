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
	Timestamp Timestamp
	Tombstone bool
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
		timestamp := m.Timestamp.Proto()
		meta.Timestamp = &timestamp
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
	if m.Revision >= meta.Revision {
		return false
	}
	if m.Timestamp != nil && meta.Timestamp != nil && !m.Timestamp.Before(meta.Timestamp) {
		return false
	}
	return true
}

func (m ObjectMeta) After(meta ObjectMeta) bool {
	if m.Revision <= meta.Revision {
		return false
	}
	if m.Timestamp != nil && meta.Timestamp != nil && !m.Timestamp.After(meta.Timestamp) {
		return false
	}
	return true
}

// Revision is a revision number
type Revision uint64
