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

package counter

import (
	"context"
	api "github.com/atomix/api/go/atomix/storage/counter"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "counter")

// RegisterRSMProxy registers the primitive server on the given node
func RegisterRSMProxy(node *rsm.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *rsm.Client) {
		api.RegisterCounterServiceServer(server, &RSMProxy{
			Proxy: rsm.NewProxy(client),
		})
	})
}

// RSMProxy is an implementation of CounterServiceServer for the counter primitive
type RSMProxy struct {
	*rsm.Proxy
}

// Create opens a new session
func (s *RSMProxy) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Debugf("Received CreateRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	err := partition.DoCreateService(ctx, request.Header)
	if err != nil {
		return nil, err
	}

	response := &api.CreateResponse{}
	log.Debugf("Sending CreateResponse %+v", response)
	return response, nil
}

// Set sets the current value of the counter
func (s *RSMProxy) Set(ctx context.Context, request *api.SetRequest) (*api.SetResponse, error) {
	log.Debugf("Received SetRequest %+v", request)

	in, err := proto.Marshal(&SetRequest{
		Value: request.Value,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opSet, in, request.Header)
	if err != nil {
		return nil, err
	}

	setResponse := &SetResponse{}
	if err = proto.Unmarshal(out, setResponse); err != nil {
		return nil, err
	}

	response := &api.SetResponse{}
	log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

// Get gets the current value of the counter
func (s *RSMProxy) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)

	in, err := proto.Marshal(&GetRequest{})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opGet, in, request.Header)
	if err != nil {
		return nil, err
	}

	getResponse := &GetResponse{}
	if err = proto.Unmarshal(out, getResponse); err != nil {
		return nil, err
	}

	response := &api.GetResponse{
		Value: getResponse.Value,
	}
	log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

// Increment increments the value of the counter by a delta
func (s *RSMProxy) Increment(ctx context.Context, request *api.IncrementRequest) (*api.IncrementResponse, error) {
	log.Debugf("Received IncrementRequest %+v", request)

	in, err := proto.Marshal(&IncrementRequest{
		Delta: request.Delta,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opIncrement, in, request.Header)
	if err != nil {
		return nil, err
	}

	incrementResponse := &IncrementResponse{}
	if err = proto.Unmarshal(out, incrementResponse); err != nil {
		return nil, err
	}

	response := &api.IncrementResponse{
		PreviousValue: incrementResponse.PreviousValue,
		NextValue:     incrementResponse.NextValue,
	}
	log.Debugf("Sending IncrementResponse %+v", response)
	return response, nil
}

// Decrement decrements the value of the counter by a delta
func (s *RSMProxy) Decrement(ctx context.Context, request *api.DecrementRequest) (*api.DecrementResponse, error) {
	log.Debugf("Received DecrementRequest %+v", request)

	in, err := proto.Marshal(&DecrementRequest{
		Delta: request.Delta,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opDecrement, in, request.Header)
	if err != nil {
		return nil, err
	}

	decrementResponse := &DecrementResponse{}
	if err = proto.Unmarshal(out, decrementResponse); err != nil {
		return nil, err
	}

	response := &api.DecrementResponse{
		PreviousValue: decrementResponse.PreviousValue,
		NextValue:     decrementResponse.NextValue,
	}
	log.Debugf("Sending DecrementResponse %+v", response)
	return response, nil
}

// CheckAndSet updates the value of the counter conditionally
func (s *RSMProxy) CheckAndSet(ctx context.Context, request *api.CheckAndSetRequest) (*api.CheckAndSetResponse, error) {
	log.Debugf("Received CheckAndSetRequest %+v", request)

	in, err := proto.Marshal(&CheckAndSetRequest{
		Expect: request.Expect,
		Update: request.Update,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opCAS, in, request.Header)
	if err != nil {
		return nil, err
	}

	casResponse := &CheckAndSetResponse{}
	if err = proto.Unmarshal(out, casResponse); err != nil {
		return nil, err
	}

	response := &api.CheckAndSetResponse{
		Succeeded: casResponse.Succeeded,
	}
	log.Debugf("Sending CheckAndSetResponse %+v", response)
	return response, nil
}

// Close closes a session
func (s *RSMProxy) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Debugf("Received CloseRequest %+v", request)
	partition := s.PartitionFor(request.Header.Primitive)
	if request.Delete {
		err := partition.DoDeleteService(ctx, request.Header)
		if err != nil {
			return nil, err
		}
		response := &api.CloseResponse{}
		log.Debugf("Sending CloseResponse %+v", response)
		return response, nil
	}

	err := partition.DoCloseService(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	response := &api.CloseResponse{}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}
