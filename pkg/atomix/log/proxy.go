// Copyright 2020-present Open Networking Foundation.
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

package log

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"google.golang.org/grpc"

	storageapi "github.com/atomix/api/go/atomix/storage"
	api "github.com/atomix/api/go/atomix/storage/log"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
)

var log = logging.GetLogger("atomix", "log")

// RegisterRSMProxy registers the election primitive on the given node
func RegisterRSMProxy(node *rsm.Node) {
	node.RegisterProxy(Type, func(server *grpc.Server, client *rsm.Client) {
		api.RegisterLogServiceServer(server, &RSMProxy{
			Proxy: rsm.NewProxy(client),
		})
	})
}

// RSMProxy is an implementation of LogServiceServer for the log primitive
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

// Close closes a session
func (s *RSMProxy) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Debugf("Received CloseRequest %+v", request)
	if request.Delete {
		partition := s.PartitionFor(request.Header.Primitive)
		err := partition.DoDeleteService(ctx, request.Header)
		if err != nil {
			return nil, err
		}
		response := &api.CloseResponse{}
		log.Debugf("Sending CloseResponse %+v", response)
		return response, nil
	}

	partition := s.PartitionFor(request.Header.Primitive)
	err := partition.DoCloseService(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	response := &api.CloseResponse{}
	log.Debugf("Sending CloseResponse %+v", response)
	return response, nil
}

// Size gets the number of entries in the log
func (s *RSMProxy) Size(ctx context.Context, request *api.SizeRequest) (*api.SizeResponse, error) {
	log.Debugf("Received SizeRequest %+v", request)
	in, err := proto.Marshal(&SizeRequest{})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opSize, in, request.Header)
	if err != nil {
		return nil, err
	}

	sizeResponse := &SizeResponse{}
	if err = proto.Unmarshal(out, sizeResponse); err != nil {
		return nil, err
	}

	response := &api.SizeResponse{
		Size_: sizeResponse.Size_,
	}
	log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

// Exists checks whether the log contains an index
func (s *RSMProxy) Exists(ctx context.Context, request *api.ExistsRequest) (*api.ExistsResponse, error) {
	log.Debugf("Received ExistsRequest %+v", request)
	in, err := proto.Marshal(&ContainsIndexRequest{
		Index: request.Index,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opExists, in, request.Header)
	if err != nil {
		return nil, err
	}

	containsResponse := &ContainsIndexResponse{}
	if err = proto.Unmarshal(out, containsResponse); err != nil {
		return nil, err
	}

	response := &api.ExistsResponse{
		ContainsIndex: containsResponse.ContainsIndex,
	}
	log.Debugf("Sending ExistsResponse %+v", response)
	return response, nil
}

// Append appends a value to the end of the log
func (s *RSMProxy) Append(ctx context.Context, request *api.AppendRequest) (*api.AppendResponse, error) {
	log.Debugf("Received PutRequest %+v", request)
	in, err := proto.Marshal(&AppendRequest{
		Index: request.Index,
		Value: request.Value,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opAppend, in, request.Header)
	if err != nil {
		return nil, err
	}

	appendResponse := &AppendResponse{}
	if err = proto.Unmarshal(out, appendResponse); err != nil {
		return nil, err
	}

	response := &api.AppendResponse{
		Index:     appendResponse.Index,
		Timestamp: appendResponse.Timestamp,
	}
	log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}

// Get gets the value of an index
func (s *RSMProxy) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Debugf("Received GetRequest %+v", request)
	in, err := proto.Marshal(&GetRequest{
		Index: request.Index,
	})
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
		Index:     getResponse.Index,
		Value:     getResponse.Value,
		Timestamp: getResponse.Timestamp,
	}
	log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

// FirstEntry gets the first entry in the log
func (s *RSMProxy) FirstEntry(ctx context.Context, request *api.FirstEntryRequest) (*api.FirstEntryResponse, error) {
	log.Debugf("Received FirstEntryRequest %+v", request)
	in, err := proto.Marshal(&FirstEntryRequest{})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opFirstEntry, in, request.Header)
	if err != nil {
		return nil, err
	}

	firstEntryResponse := &FirstEntryResponse{}
	if err = proto.Unmarshal(out, firstEntryResponse); err != nil {
		return nil, err
	}

	response := &api.FirstEntryResponse{
		Index:     firstEntryResponse.Index,
		Value:     firstEntryResponse.Value,
		Timestamp: firstEntryResponse.Timestamp,
	}
	log.Debugf("Sending FirstEntryResponse %+v", response)
	return response, nil
}

// LastEntry gets the last entry in the log
func (s *RSMProxy) LastEntry(ctx context.Context, request *api.LastEntryRequest) (*api.LastEntryResponse, error) {
	log.Debugf("Received LastEntryRequest %+v", request)
	in, err := proto.Marshal(&LastEntryRequest{})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opLastEntry, in, request.Header)
	if err != nil {
		return nil, err
	}

	lastEntryResponse := &LastEntryResponse{}
	if err = proto.Unmarshal(out, lastEntryResponse); err != nil {
		return nil, err
	}

	response := &api.LastEntryResponse{
		Index:     lastEntryResponse.Index,
		Value:     lastEntryResponse.Value,
		Timestamp: lastEntryResponse.Timestamp,
	}
	log.Debugf("Sending LastEntryResponse %+v", response)
	return response, nil
}

// PrevEntry gets the previous entry in the log
func (s *RSMProxy) PrevEntry(ctx context.Context, request *api.PrevEntryRequest) (*api.PrevEntryResponse, error) {
	log.Debugf("Received PrevEntryRequest %+v", request)
	in, err := proto.Marshal(&PrevEntryRequest{
		Index: request.Index,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opPrevEntry, in, request.Header)
	if err != nil {
		return nil, err
	}

	prevEntryResponse := &PrevEntryResponse{}
	if err = proto.Unmarshal(out, prevEntryResponse); err != nil {
		return nil, err
	}

	response := &api.PrevEntryResponse{
		Index:     prevEntryResponse.Index,
		Value:     prevEntryResponse.Value,
		Timestamp: prevEntryResponse.Timestamp,
	}
	log.Debugf("Sending PrevEntryResponse %+v", response)
	return response, nil
}

// NextEntry gets the next entry in the log
func (s *RSMProxy) NextEntry(ctx context.Context, request *api.NextEntryRequest) (*api.NextEntryResponse, error) {
	log.Debugf("Received NextEntryRequest %+v", request)
	in, err := proto.Marshal(&NextEntryRequest{
		Index: request.Index,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoQuery(ctx, opNextEntry, in, request.Header)
	if err != nil {
		return nil, err
	}

	nextEntryResponse := &NextEntryResponse{}
	if err = proto.Unmarshal(out, nextEntryResponse); err != nil {
		return nil, err
	}

	response := &api.NextEntryResponse{
		Index:     nextEntryResponse.Index,
		Value:     nextEntryResponse.Value,
		Timestamp: nextEntryResponse.Timestamp,
	}
	log.Debugf("Sending NextEntryResponse %+v", response)
	return response, nil
}

// Remove removes a key from the log
func (s *RSMProxy) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	log.Debugf("Received RemoveRequest %+v", request)
	in, err := proto.Marshal(&RemoveRequest{
		Index: request.Index,
		Value: request.Value,
	})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opRemove, in, request.Header)
	if err != nil {
		return nil, err
	}

	removeResponse := &RemoveResponse{}
	if err = proto.Unmarshal(out, removeResponse); err != nil {
		return nil, err
	}

	response := &api.RemoveResponse{
		Index:         removeResponse.Index,
		PreviousValue: removeResponse.PreviousValue,
	}
	log.Debugf("Sending RemoveRequest %+v", response)
	return response, nil
}

// Events listens for log change events
func (s *RSMProxy) Events(request *api.EventRequest, srv api.LogService_EventsServer) error {
	log.Debugf("Received EventRequest %+v", request)
	in, err := proto.Marshal(&ListenRequest{
		Replay: request.Replay,
		Index:  request.Index,
	})
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	partition := s.PartitionFor(request.Header.Primitive)
	if err := partition.DoCommandStream(srv.Context(), opEvents, in, request.Header, stream); err != nil {
		return err
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			return result.Error
		}

		response := &ListenResponse{}
		output := result.Value.(rsm.SessionOutput)
		if err = proto.Unmarshal(output.Value.([]byte), response); err != nil {
			return err
		}

		var eventResponse *api.EventResponse
		switch output.Type {
		case storageapi.ResponseType_OPEN_STREAM:
			eventResponse = &api.EventResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_OPEN_STREAM,
				},
			}
		case storageapi.ResponseType_CLOSE_STREAM:
			eventResponse = &api.EventResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_CLOSE_STREAM,
				},
			}
		default:
			eventResponse = &api.EventResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_RESPONSE,
				},
				Type:      getEventType(response.Type),
				Index:     response.Index,
				Value:     response.Value,
				Timestamp: response.Timestamp,
			}
		}

		log.Debugf("Sending EventResponse %+v", eventResponse)
		if err = srv.Send(eventResponse); err != nil {
			return err
		}
	}
	log.Debugf("Finished EventRequest %+v", request)
	return nil
}

// Entries lists all entries currently in the log
func (s *RSMProxy) Entries(request *api.EntriesRequest, srv api.LogService_EntriesServer) error {
	log.Debugf("Received EntriesRequest %+v", request)
	in, err := proto.Marshal(&EntriesRequest{})
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	partition := s.PartitionFor(request.Header.Primitive)
	if err := partition.DoQueryStream(srv.Context(), opEntries, in, request.Header, stream); err != nil {
		log.Errorf("EntriesRequest failed: %v", err)
		return err
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			log.Errorf("EntriesRequest failed: %v", result.Error)
			return result.Error
		}

		response := &EntriesResponse{}
		output := result.Value.(rsm.SessionOutput)
		if err = proto.Unmarshal(output.Value.([]byte), response); err != nil {
			return err
		}

		var entriesResponse *api.EntriesResponse
		switch output.Type {
		case storageapi.ResponseType_OPEN_STREAM:
			entriesResponse = &api.EntriesResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_OPEN_STREAM,
				},
			}
		case storageapi.ResponseType_CLOSE_STREAM:
			entriesResponse = &api.EntriesResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_CLOSE_STREAM,
				},
			}
		default:
			entriesResponse = &api.EntriesResponse{
				Header: storageapi.ResponseHeader{
					Type: storageapi.ResponseType_RESPONSE,
				},
				Index:     response.Index,
				Value:     response.Value,
				Timestamp: response.Timestamp,
			}
		}

		log.Debugf("Sending EntriesResponse %+v", entriesResponse)
		if err = srv.Send(entriesResponse); err != nil {
			return err
		}
	}
	log.Debugf("Finished EntriesRequest %+v", request)
	return nil
}

// Clear removes all keys from the log
func (s *RSMProxy) Clear(ctx context.Context, request *api.ClearRequest) (*api.ClearResponse, error) {
	log.Debugf("Received ClearRequest %+v", request)
	in, err := proto.Marshal(&ClearRequest{})
	if err != nil {
		return nil, err
	}

	partition := s.PartitionFor(request.Header.Primitive)
	out, err := partition.DoCommand(ctx, opClear, in, request.Header)
	if err != nil {
		return nil, err
	}

	serviceResponse := &ClearResponse{}
	if err = proto.Unmarshal(out, serviceResponse); err != nil {
		return nil, err
	}

	response := &api.ClearResponse{}
	log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func getEventType(eventType ListenResponse_Type) api.EventResponse_Type {
	switch eventType {
	case ListenResponse_NONE:
		return api.EventResponse_NONE
	case ListenResponse_APPENDED:
		return api.EventResponse_APPENDED
	case ListenResponse_REMOVED:
		return api.EventResponse_REMOVED
	}
	return api.EventResponse_NONE
}