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

package headers

import (
	"fmt"
	"google.golang.org/grpc/metadata"
	"strconv"
	"time"
)

// Header is an Atomix header
type Header string

const (
	PrimitiveType Header = "Atomix-Primitive-Type"
	PrimitiveName Header = "Atomix-Primitive-Name"
	ServiceType   Header = "Atomix-Service-Type"
	ServiceID     Header = "Atomix-Service-Id"
	PartitionID   Header = "Atomix-Partition-Id"
	Timestamp     Header = "Atomix-Timestamp"
)

func (h Header) Set(md metadata.MD, value interface{}) {
	md[h.String()] = append(md[h.String()], fmt.Sprint(value))
}

func (h Header) SetString(md metadata.MD, value string) {
	md[h.String()] = append(md[h.String()], value)
}

func (h Header) SetBool(md metadata.MD, value bool) {
	md[h.String()] = append(md[h.String()], strconv.FormatBool(value))
}

func (h Header) SetInt(md metadata.MD, value int) {
	h.Set(md, value)
}

func (h Header) SetInt32(md metadata.MD, value int32) {
	h.Set(md, value)
}

func (h Header) SetInt64(md metadata.MD, value int64) {
	h.Set(md, value)
}

func (h Header) SetUint(md metadata.MD, value uint) {
	h.Set(md, value)
}

func (h Header) SetUint32(md metadata.MD, value uint32) {
	h.Set(md, value)
}

func (h Header) SetUint64(md metadata.MD, value uint64) {
	h.Set(md, value)
}

func (h Header) SetFloat32(md metadata.MD, value float32) {
	h.Set(md, value)
}

func (h Header) SetFloat64(md metadata.MD, value float64) {
	h.Set(md, value)
}

func (h Header) SetTime(md metadata.MD, value time.Time) {
	h.SetInt64(md, value.UnixNano())
}

func (h Header) Sets(md metadata.MD, values ...interface{}) {
	for _, value := range values {
		h.Set(md, value)
	}
}

func (h Header) SetStrings(md metadata.MD, values ...string) {
	for _, value := range values {
		h.SetString(md, value)
	}
}

func (h Header) SetInts(md metadata.MD, values ...int) {
	for _, value := range values {
		h.SetInt(md, value)
	}
}

func (h Header) Add(md metadata.MD, value interface{}) {
	md[h.String()] = append(md[h.String()], fmt.Sprint(value))
}

func (h Header) AddString(md metadata.MD, value string) {
	md[h.String()] = append(md[h.String()], value)
}

func (h Header) AddBool(md metadata.MD, value bool) {
	md[h.String()] = append(md[h.String()], strconv.FormatBool(value))
}

func (h Header) AddInt(md metadata.MD, value int) {
	h.Add(md, value)
}

func (h Header) AddInt32(md metadata.MD, value int32) {
	h.Add(md, value)
}

func (h Header) AddInt64(md metadata.MD, value int64) {
	h.Add(md, value)
}

func (h Header) AddUint(md metadata.MD, value uint) {
	h.Add(md, value)
}

func (h Header) AddUint32(md metadata.MD, value uint32) {
	h.Add(md, value)
}

func (h Header) AddUint64(md metadata.MD, value uint64) {
	h.Add(md, value)
}

func (h Header) AddFloat32(md metadata.MD, value float32) {
	h.Add(md, value)
}

func (h Header) AddFloat64(md metadata.MD, value float64) {
	h.Add(md, value)
}

func (h Header) AddTime(md metadata.MD, value time.Time) {
	h.AddInt64(md, value.UnixNano())
}

func (h Header) Adds(md metadata.MD, values ...interface{}) {
	for _, value := range values {
		h.Add(md, value)
	}
}

func (h Header) AddStrings(md metadata.MD, values ...string) {
	for _, value := range values {
		h.AddString(md, value)
	}
}

func (h Header) AddInts(md metadata.MD, values ...int) {
	for _, value := range values {
		h.AddInt(md, value)
	}
}

func (h Header) Get(md metadata.MD) (string, bool) {
	values := md.Get(h.String())
	if len(values) == 0 {
		return "", false
	}
	return values[0], true
}

func (h Header) GetString(md metadata.MD) (string, bool) {
	return h.Get(md)
}

func (h Header) GetBool(md metadata.MD) (bool, bool) {
	value, ok := h.Get(md)
	if !ok {
		return false, false
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		return false, false
	}
	return b, true
}

func (h Header) GetInt(md metadata.MD) (int, bool) {
	value, ok := h.Get(md)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return int(i), true
}

func (h Header) GetInt32(md metadata.MD) (int32, bool) {
	value, ok := h.Get(md)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(i), true
}

func (h Header) GetInt64(md metadata.MD) (int64, bool) {
	value, ok := h.Get(md)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return int64(i), true
}

func (h Header) GetUint(md metadata.MD) (uint, bool) {
	value, ok := h.Get(md)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return uint(i), true
}

func (h Header) GetUint32(md metadata.MD) (uint32, bool) {
	value, ok := h.Get(md)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(i), true
}

func (h Header) GetUint64(md metadata.MD) (uint64, bool) {
	value, ok := h.Get(md)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return uint64(i), true
}

func (h Header) GetFloat32(md metadata.MD) (float32, bool) {
	value, ok := h.Get(md)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(value, 32)
	if err != nil {
		return 0, false
	}
	return float32(f), true
}

func (h Header) GetFloat64(md metadata.MD) (float64, bool) {
	value, ok := h.Get(md)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, false
	}
	return float64(f), true
}

func (h Header) GetTime(md metadata.MD) (time.Time, bool) {
	i, ok := h.GetInt64(md)
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(0, i), true
}

func (h Header) Gets(md metadata.MD) ([]string, bool) {
	values := md.Get(h.String())
	if len(values) == 0 {
		return nil, false
	}
	return values, true
}

func (h Header) GetStrings(md metadata.MD) ([]string, bool) {
	return h.Gets(md)
}

func (h Header) GetInts(md metadata.MD) ([]int, bool) {
	values, ok := h.Gets(md)
	if !ok {
		return nil, false
	}
	ints := make([]int, len(values))
	for i := 0; i < len(values); i++ {
		value, err := strconv.ParseInt(values[i], 10, 64)
		if err != nil {
			return nil, false
		}
		ints[i] = int(value)
	}
	return ints, true
}

func (h Header) Name() string {
	return string(h)
}

func (h Header) String() string {
	return h.Name()
}
