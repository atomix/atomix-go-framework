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

package value

import (
	"context"
	valueapi "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
)

func init() {
	registerService(func(protocol Protocol) Service {
		return &valueService{
			protocol: protocol,
		}
	})
}

type valueService struct {
	protocol Protocol
	value    *valueapi.Value
}

func (s *valueService) Protocol() Protocol {
	return s.protocol
}

func (s *valueService) Read(ctx context.Context) (*valueapi.Value, error) {
	return s.value, nil
}

func (s *valueService) Update(ctx context.Context, value *valueapi.Value) error {
	if meta.New(value.ObjectMeta).After(meta.New(s.value.ObjectMeta)) {
		s.value = value
	}
	return nil
}

func (s *valueService) Set(ctx context.Context, input *valueapi.SetRequest) (*valueapi.SetResponse, error) {
	if s.value != nil && meta.New(s.value.ObjectMeta).After(meta.New(input.Value.ObjectMeta)) {
		return &valueapi.SetResponse{
			Value: *s.value,
		}, nil
	}

	err := checkPreconditions(s.value, input.Preconditions)
	if err != nil {
		return nil, err
	}

	s.value = &input.Value
	err = s.Protocol().Broadcast(ctx, &input.Value)
	if err != nil {
		return nil, err
	}
	return &valueapi.SetResponse{
		Value: input.Value,
	}, nil
}

func (s *valueService) Get(ctx context.Context, input *valueapi.GetRequest) (*valueapi.GetResponse, error) {
	var value valueapi.Value
	if s.value != nil {
		value = *s.value
	}
	return &valueapi.GetResponse{
		Value: value,
	}, nil
}

func (s *valueService) Events(ctx context.Context, request *valueapi.EventsRequest, responses chan<- valueapi.EventsResponse) error {
	panic("implement me")
}

func checkPreconditions(value *valueapi.Value, preconditions []valueapi.Precondition) error {
	for _, precondition := range preconditions {
		switch p := precondition.Precondition.(type) {
		case *valueapi.Precondition_Metadata:
			if value == nil {
				return errors.NewConflict("metadata precondition failed")
			}
			if !meta.New(value.ObjectMeta).Equal(meta.New(*p.Metadata)) {
				return errors.NewConflict("metadata mismatch")
			}
		}
	}
	return nil
}
