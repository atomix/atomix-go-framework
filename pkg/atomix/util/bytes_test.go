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

package util

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteReadSlice(t *testing.T) {
	buf := &bytes.Buffer{}
	slice := []*TestStruct{
		{
			Foo: "bar",
			Bar: 1,
		},
		{
			Foo: "baz",
			Bar: 2,
		},
	}
	err := WriteSlice(buf, slice, func(value *TestStruct) ([]byte, error) {
		return json.Marshal(value)
	})
	assert.NoError(t, err)
	data := buf.Bytes()

	buf = bytes.NewBuffer(data)
	slice = make([]*TestStruct, 2)
	err = ReadSlice(buf, slice, func(data []byte) (*TestStruct, error) {
		s := &TestStruct{}
		if err := json.Unmarshal(data, s); err != nil {
			return nil, err
		}
		return s, nil
	})
	assert.NoError(t, err)
	assert.Len(t, slice, 2)
}

func TestWriteReadMap(t *testing.T) {
	buf := &bytes.Buffer{}
	entries := map[string]*TestStruct{
		"foo": {
			Foo: "bar",
			Bar: 1,
		},
		"bar": {
			Foo: "baz",
			Bar: 2,
		},
	}
	err := WriteMap(buf, entries, func(key string, value *TestStruct) ([]byte, error) {
		return json.Marshal(&TestEntry{
			Key:   key,
			Value: value,
		})
	})
	assert.NoError(t, err)
	data := buf.Bytes()

	buf = bytes.NewBuffer(data)
	entries = make(map[string]*TestStruct)
	err = ReadMap(buf, entries, func(data []byte) (string, *TestStruct, error) {
		e := &TestEntry{}
		if err := json.Unmarshal(data, e); err != nil {
			return "", nil, err
		}
		return e.Key, e.Value, nil
	})
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
}

type TestEntry struct {
	Key   string
	Value *TestStruct
}

type TestStruct struct {
	Foo string
	Bar int
}
