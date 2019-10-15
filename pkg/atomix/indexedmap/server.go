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

package indexedmap

import (
	"context"
	"github.com/atomix/atomix-api/proto/atomix/headers"
	api "github.com/atomix/atomix-api/proto/atomix/indexedmap"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/server"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func init() {
	node.RegisterServer(registerServer)
}

// registerServer registers a map server with the given gRPC server
func registerServer(server *grpc.Server, protocol node.Protocol) {
	api.RegisterIndexedMapServiceServer(server, newServer(protocol.Client()))
}

func newServer(client node.Client) api.IndexedMapServiceServer {
	return &Server{
		SessionizedServer: &server.SessionizedServer{
			Type:   "indexedmap",
			Client: client,
		},
	}
}

// Server is an implementation of MapServiceServer for the map primitive
type Server struct {
	api.IndexedMapServiceServer
	*server.SessionizedServer
}

// Create opens a new session
func (m *Server) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Tracef("Received CreateRequest %+v", request)
	session, err := m.OpenSession(ctx, request.Header, request.Timeout)
	if err != nil {
		return nil, err
	}
	response := &api.CreateResponse{
		Header: &headers.ResponseHeader{
			SessionID: session,
			Index:     session,
		},
	}
	log.Tracef("Sending CreateResponse %+v", response)
	return response, nil
}

// KeepAlive keeps an existing session alive
func (m *Server) KeepAlive(ctx context.Context, request *api.KeepAliveRequest) (*api.KeepAliveResponse, error) {
	log.Tracef("Received KeepAliveRequest %+v", request)
	if err := m.KeepAliveSession(ctx, request.Header); err != nil {
		return nil, err
	}
	response := &api.KeepAliveResponse{
		Header: &headers.ResponseHeader{
			SessionID: request.Header.SessionID,
		},
	}
	log.Tracef("Sending KeepAliveResponse %+v", response)
	return response, nil
}

// Close closes a session
func (m *Server) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Tracef("Received CloseRequest %+v", request)
	if request.Delete {
		if err := m.Delete(ctx, request.Header); err != nil {
			return nil, err
		}
	} else {
		if err := m.CloseSession(ctx, request.Header); err != nil {
			return nil, err
		}
	}

	response := &api.CloseResponse{
		Header: &headers.ResponseHeader{
			SessionID: request.Header.SessionID,
		},
	}
	log.Tracef("Sending CloseResponse %+v", response)
	return response, nil
}

// Size gets the number of entries in the map
func (m *Server) Size(ctx context.Context, request *api.SizeRequest) (*api.SizeResponse, error) {
	log.Tracef("Received SizeRequest %+v", request)
	in, err := proto.Marshal(&SizeRequest{})
	if err != nil {
		return nil, err
	}

	out, header, err := m.Query(ctx, "size", in, request.Header)
	if err != nil {
		return nil, err
	}

	sizeResponse := &SizeResponse{}
	if err = proto.Unmarshal(out, sizeResponse); err != nil {
		return nil, err
	}

	response := &api.SizeResponse{
		Header: header,
		Size_:  sizeResponse.Size_,
	}
	log.Tracef("Sending SizeResponse %+v", response)
	return response, nil
}

// Exists checks whether the map contains a key
func (m *Server) Exists(ctx context.Context, request *api.ExistsRequest) (*api.ExistsResponse, error) {
	log.Tracef("Received ExistsRequest %+v", request)
	in, err := proto.Marshal(&ContainsKeyRequest{
		Key: request.Key,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := m.Query(ctx, "exists", in, request.Header)
	if err != nil {
		return nil, err
	}

	containsResponse := &ContainsKeyResponse{}
	if err = proto.Unmarshal(out, containsResponse); err != nil {
		return nil, err
	}

	response := &api.ExistsResponse{
		Header:      header,
		ContainsKey: containsResponse.ContainsKey,
	}
	log.Tracef("Sending ExistsResponse %+v", response)
	return response, nil
}

// Put puts a key/value pair into the map
func (m *Server) Put(ctx context.Context, request *api.PutRequest) (*api.PutResponse, error) {
	log.Tracef("Received PutRequest %+v", request)
	in, err := proto.Marshal(&PutRequest{
		Index:   uint64(request.Index),
		Key:     request.Key,
		Value:   request.Value,
		Version: uint64(request.Version),
		TTL:     request.TTL,
		IfEmpty: request.Version == -1,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := m.Command(ctx, "put", in, request.Header)
	if err != nil {
		return nil, err
	}

	putResponse := &PutResponse{}
	if err = proto.Unmarshal(out, putResponse); err != nil {
		return nil, err
	}

	response := &api.PutResponse{
		Header:          header,
		Status:          getResponseStatus(putResponse.Status),
		Index:           int64(putResponse.Index),
		Key:             putResponse.Key,
		Created:         putResponse.Created,
		Updated:         putResponse.Updated,
		PreviousValue:   putResponse.PreviousValue,
		PreviousVersion: int64(putResponse.PreviousVersion),
	}
	log.Tracef("Sending PutResponse %+v", response)
	return response, nil
}

// Replace replaces a key/value pair in the map
func (m *Server) Replace(ctx context.Context, request *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	log.Tracef("Received ReplaceRequest %+v", request)
	in, err := proto.Marshal(&ReplaceRequest{
		Index:           uint64(request.Index),
		Key:             request.Key,
		PreviousValue:   request.PreviousValue,
		PreviousVersion: uint64(request.PreviousVersion),
		NewValue:        request.NewValue,
		TTL:             request.TTL,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := m.Command(ctx, "replace", in, request.Header)
	if err != nil {
		return nil, err
	}

	replaceResponse := &ReplaceResponse{}
	if err = proto.Unmarshal(out, replaceResponse); err != nil {
		return nil, err
	}

	response := &api.ReplaceResponse{
		Header:          header,
		Status:          getResponseStatus(replaceResponse.Status),
		Index:           int64(replaceResponse.Index),
		Key:             replaceResponse.Key,
		Created:         replaceResponse.Created,
		Updated:         replaceResponse.Updated,
		PreviousValue:   replaceResponse.PreviousValue,
		PreviousVersion: int64(replaceResponse.PreviousVersion),
	}
	log.Tracef("Sending ReplaceResponse %+v", response)
	return response, nil
}

// Get gets the value of a key
func (m *Server) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Tracef("Received GetRequest %+v", request)
	in, err := proto.Marshal(&GetRequest{
		Index: uint64(request.Index),
		Key:   request.Key,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := m.Query(ctx, "get", in, request.Header)
	if err != nil {
		return nil, err
	}

	getResponse := &GetResponse{}
	if err = proto.Unmarshal(out, getResponse); err != nil {
		return nil, err
	}

	response := &api.GetResponse{
		Header:  header,
		Index:   int64(getResponse.Index),
		Key:     getResponse.Key,
		Value:   getResponse.Value,
		Version: int64(getResponse.Version),
		Created: getResponse.Created,
		Updated: getResponse.Updated,
	}
	log.Tracef("Sending GetRequest %+v", response)
	return response, nil
}

// Remove removes a key from the map
func (m *Server) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	log.Tracef("Received RemoveRequest %+v", request)
	in, err := proto.Marshal(&RemoveRequest{
		Index:   uint64(request.Index),
		Key:     request.Key,
		Value:   request.Value,
		Version: uint64(request.Version),
	})
	if err != nil {
		return nil, err
	}

	out, header, err := m.Command(ctx, "remove", in, request.Header)
	if err != nil {
		return nil, err
	}

	removeResponse := &RemoveResponse{}
	if err = proto.Unmarshal(out, removeResponse); err != nil {
		return nil, err
	}

	response := &api.RemoveResponse{
		Header:          header,
		Status:          getResponseStatus(removeResponse.Status),
		Index:           int64(removeResponse.Index),
		Key:             removeResponse.Key,
		PreviousValue:   removeResponse.PreviousValue,
		PreviousVersion: int64(removeResponse.PreviousVersion),
	}
	log.Tracef("Sending RemoveRequest %+v", response)
	return response, nil
}

// Clear removes all keys from the map
func (m *Server) Clear(ctx context.Context, request *api.ClearRequest) (*api.ClearResponse, error) {
	log.Tracef("Received ClearRequest %+v", request)
	in, err := proto.Marshal(&ClearRequest{})
	if err != nil {
		return nil, err
	}

	out, header, err := m.Command(ctx, "clear", in, request.Header)
	if err != nil {
		return nil, err
	}

	serviceResponse := &ClearResponse{}
	if err = proto.Unmarshal(out, serviceResponse); err != nil {
		return nil, err
	}

	response := &api.ClearResponse{
		Header: header,
	}
	log.Tracef("Sending ClearResponse %+v", response)
	return response, nil
}

// Events listens for map change events
func (m *Server) Events(request *api.EventRequest, srv api.IndexedMapService_EventsServer) error {
	log.Tracef("Received EventRequest %+v", request)
	in, err := proto.Marshal(&ListenRequest{
		Replay: request.Replay,
	})
	if err != nil {
		return err
	}

	ch := make(chan server.SessionOutput)
	if err := m.CommandStream("events", in, request.Header, ch); err != nil {
		return err
	}

	for result := range ch {
		if result.Failed() {
			return result.Error
		}

		response := &ListenResponse{}
		if err = proto.Unmarshal(result.Value, response); err != nil {
			return err
		}
		eventResponse := &api.EventResponse{
			Header:  result.Header,
			Type:    getEventType(response.Type),
			Index:   int64(response.Index),
			Key:     response.Key,
			Value:   response.Value,
			Version: int64(response.Version),
			Created: response.Created,
			Updated: response.Updated,
		}
		log.Tracef("Sending EventResponse %+v", response)
		if err = srv.Send(eventResponse); err != nil {
			return err
		}
	}
	log.Tracef("Finished EventRequest %+v", request)
	return nil
}

// Entries lists all entries currently in the map
func (m *Server) Entries(request *api.EntriesRequest, srv api.IndexedMapService_EntriesServer) error {
	log.Tracef("Received EntriesRequest %+v", request)
	in, err := proto.Marshal(&EntriesRequest{})
	if err != nil {
		return err
	}

	ch := make(chan server.SessionOutput)
	if err := m.QueryStream("entries", in, request.Header, ch); err != nil {
		return err
	}

	for result := range ch {
		if result.Failed() {
			return result.Error
		}

		response := &EntriesResponse{}
		if err = proto.Unmarshal(result.Value, response); err != nil {
			return err
		}
		entriesResponse := &api.EntriesResponse{
			Header:  result.Header,
			Index:   int64(response.Index),
			Key:     response.Key,
			Value:   response.Value,
			Version: int64(response.Version),
			Created: response.Created,
			Updated: response.Updated,
		}
		log.Tracef("Sending EntriesResponse %+v", response)
		if err = srv.Send(entriesResponse); err != nil {
			return err
		}
	}
	log.Tracef("Finished EntriesRequest %+v", request)
	return nil
}

func getResponseStatus(status UpdateStatus) api.ResponseStatus {
	switch status {
	case UpdateStatus_OK:
		return api.ResponseStatus_OK
	case UpdateStatus_NOOP:
		return api.ResponseStatus_NOOP
	case UpdateStatus_PRECONDITION_FAILED:
		return api.ResponseStatus_PRECONDITION_FAILED
	case UpdateStatus_WRITE_LOCK:
		return api.ResponseStatus_WRITE_LOCK
	}
	return api.ResponseStatus_OK
}

func getEventType(eventType ListenResponse_Type) api.EventResponse_Type {
	switch eventType {
	case ListenResponse_NONE:
		return api.EventResponse_NONE
	case ListenResponse_INSERTED:
		return api.EventResponse_INSERTED
	case ListenResponse_UPDATED:
		return api.EventResponse_UPDATED
	case ListenResponse_REMOVED:
		return api.EventResponse_REMOVED
	default:
		return api.EventResponse_OPEN
	}
}
