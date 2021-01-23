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
	"context"
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

func (h Header) Set(ctx context.Context, value interface{}) context.Context {
	return metadata.AppendToOutgoingContext(ctx, h.String(), fmt.Sprint(value))
}

func (h Header) SetString(ctx context.Context, value string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, h.String(), value)
}

func (h Header) SetBool(ctx context.Context, value bool) context.Context {
	return metadata.AppendToOutgoingContext(ctx, h.String(), strconv.FormatBool(value))
}

func (h Header) SetInt(ctx context.Context, value int) context.Context {
	return h.Set(ctx, value)
}

func (h Header) SetInt32(ctx context.Context, value int32) context.Context {
	return h.Set(ctx, value)
}

func (h Header) SetInt64(ctx context.Context, value int64) context.Context {
	return h.Set(ctx, value)
}

func (h Header) SetUint(ctx context.Context, value uint) context.Context {
	return h.Set(ctx, value)
}

func (h Header) SetUint32(ctx context.Context, value uint32) context.Context {
	return h.Set(ctx, value)
}

func (h Header) SetUint64(ctx context.Context, value uint64) context.Context {
	return h.Set(ctx, value)
}

func (h Header) SetFloat32(ctx context.Context, value float32) context.Context {
	return h.Set(ctx, value)
}

func (h Header) SetFloat64(ctx context.Context, value float64) context.Context {
	return h.Set(ctx, value)
}

func (h Header) SetTime(ctx context.Context, value time.Time) context.Context {
	return h.SetInt64(ctx, value.UnixNano())
}

func (h Header) Sets(ctx context.Context, values ...interface{}) context.Context {
	for _, value := range values {
		ctx = h.Set(ctx, value)
	}
	return ctx
}

func (h Header) SetStrings(ctx context.Context, values ...string) context.Context {
	for _, value := range values {
		ctx = h.SetString(ctx, value)
	}
	return ctx
}

func (h Header) SetInts(ctx context.Context, values ...int) context.Context {
	for _, value := range values {
		ctx = h.SetInt(ctx, value)
	}
	return ctx
}

func (h Header) Add(ctx context.Context, value interface{}) context.Context {
	return metadata.AppendToOutgoingContext(ctx, h.String(), fmt.Sprint(value))
}

func (h Header) AddString(ctx context.Context, value string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, h.String(), value)
}

func (h Header) AddBool(ctx context.Context, value bool) context.Context {
	return metadata.AppendToOutgoingContext(ctx, h.String(), strconv.FormatBool(value))
}

func (h Header) AddInt(ctx context.Context, value int) context.Context {
	return h.Add(ctx, value)
}

func (h Header) AddInt32(ctx context.Context, value int32) context.Context {
	return h.Add(ctx, value)
}

func (h Header) AddInt64(ctx context.Context, value int64) context.Context {
	return h.Add(ctx, value)
}

func (h Header) AddUint(ctx context.Context, value uint) context.Context {
	return h.Add(ctx, value)
}

func (h Header) AddUint32(ctx context.Context, value uint32) context.Context {
	return h.Add(ctx, value)
}

func (h Header) AddUint64(ctx context.Context, value uint64) context.Context {
	return h.Add(ctx, value)
}

func (h Header) AddFloat32(ctx context.Context, value float32) context.Context {
	return h.Add(ctx, value)
}

func (h Header) AddFloat64(ctx context.Context, value float64) context.Context {
	return h.Add(ctx, value)
}

func (h Header) AddTime(ctx context.Context, value time.Time) context.Context {
	return h.AddInt64(ctx, value.UnixNano())
}

func (h Header) Adds(ctx context.Context, values ...interface{}) context.Context {
	for _, value := range values {
		ctx = h.Add(ctx, value)
	}
	return ctx
}

func (h Header) AddStrings(ctx context.Context, values ...string) context.Context {
	for _, value := range values {
		ctx = h.AddString(ctx, value)
	}
	return ctx
}

func (h Header) AddInts(ctx context.Context, values ...int) context.Context {
	for _, value := range values {
		ctx = h.AddInt(ctx, value)
	}
	return ctx
}

func (h Header) Get(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	values := md.Get(h.String())
	if len(values) == 0 {
		return "", false
	}
	return values[0], true
}

func (h Header) GetString(ctx context.Context) (string, bool) {
	return h.Get(ctx)
}

func (h Header) GetBool(ctx context.Context) (bool, bool) {
	value, ok := h.Get(ctx)
	if !ok {
		return false, false
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		return false, false
	}
	return b, true
}

func (h Header) GetInt(ctx context.Context) (int, bool) {
	value, ok := h.Get(ctx)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return int(i), true
}

func (h Header) GetInt32(ctx context.Context) (int32, bool) {
	value, ok := h.Get(ctx)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(i), true
}

func (h Header) GetInt64(ctx context.Context) (int64, bool) {
	value, ok := h.Get(ctx)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return int64(i), true
}

func (h Header) GetUint(ctx context.Context) (uint, bool) {
	value, ok := h.Get(ctx)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return uint(i), true
}

func (h Header) GetUint32(ctx context.Context) (uint32, bool) {
	value, ok := h.Get(ctx)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(i), true
}

func (h Header) GetUint64(ctx context.Context) (uint64, bool) {
	value, ok := h.Get(ctx)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return uint64(i), true
}

func (h Header) GetFloat32(ctx context.Context) (float32, bool) {
	value, ok := h.Get(ctx)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(value, 32)
	if err != nil {
		return 0, false
	}
	return float32(f), true
}

func (h Header) GetFloat64(ctx context.Context) (float64, bool) {
	value, ok := h.Get(ctx)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, false
	}
	return float64(f), true
}

func (h Header) GetTime(ctx context.Context) (time.Time, bool) {
	i, ok := h.GetInt64(ctx)
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(0, i), true
}

func (h Header) Gets(ctx context.Context) ([]string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, false
	}
	values := md.Get(h.String())
	if len(values) == 0 {
		return nil, false
	}
	return values, true
}

func (h Header) GetStrings(ctx context.Context) ([]string, bool) {
	return h.Gets(ctx)
}

func (h Header) GetInts(ctx context.Context) ([]int, bool) {
	values, ok := h.Gets(ctx)
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
