// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteReadBytes(t *testing.T) {
	data := []byte{}
	buf := &bytes.Buffer{}
	err := WriteBytes(buf, data)
	assert.NoError(t, err)
	data = buf.Bytes()

	buf = bytes.NewBuffer(data)
	data, err = ReadBytes(buf)
	assert.NoError(t, err)
	assert.Len(t, data, 0)

	data = []byte{}
	buf = &bytes.Buffer{}
	err = WriteBytes(buf, data)
	assert.NoError(t, err)
	data = buf.Bytes()

	data, err = ReadBytes(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Len(t, data, 0)

	data = []byte{1, 2, 3}
	buf = &bytes.Buffer{}
	err = WriteBytes(buf, data)
	assert.NoError(t, err)
	data = buf.Bytes()

	buf = bytes.NewBuffer(data)
	data, err = ReadBytes(buf)
	assert.NoError(t, err)
	assert.Len(t, data, 3)

	data = []byte{1, 2, 3}
	buf = &bytes.Buffer{}
	err = WriteBytes(buf, data)
	assert.NoError(t, err)
	data = buf.Bytes()

	data, err = ReadBytes(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Len(t, data, 3)
}

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
