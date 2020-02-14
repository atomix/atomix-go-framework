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

	"github.com/atomix/api/proto/atomix/headers"
	api "github.com/atomix/api/proto/atomix/indexedmap"
	logapi "github.com/atomix/api/proto/atomix/log"
	mapapi "github.com/atomix/api/proto/atomix/map"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/server"
	"github.com/atomix/go-framework/pkg/atomix/service"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func init() {
	node.RegisterServer(registerServer)
}

// registerServer registers a map server with the given gRPC server
func registerServer(server *grpc.Server, protocol node.Protocol) {
	api.RegisterIndexedMapServiceServer(server, newServer(protocol))
}

func newServer(protocol node.Protocol) api.IndexedMapServiceServer {
	return &Server{
		Server: &server.Server{
			Type:     service.ServiceType_INDEXED_MAP,
			Protocol: protocol,
		},
	}
}

// Server is an implementation of MapServiceServer for the map primitive
type Server struct {
	api.IndexedMapServiceServer
	*server.Server
}

// Create opens a new session
func (s *Server) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Tracef("Received CreateRequest %+v", request)
	header, err := s.DoCreateService(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	response := &api.CreateResponse{
		Header: header,
	}
	log.Tracef("Sending CreateResponse %+v", response)
	return response, nil
}

// Close closes a session
func (s *Server) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Tracef("Received CloseRequest %+v", request)
	mapCloseRequest := mapapi.CloseRequest{
		Delete: request.GetDelete(),
	}
	logCloseRequest := logapi.CloseRequest{
		Delete: request.GetDelete(),
	}

	if mapCloseRequest.Delete && logCloseRequest.Delete {
		_, err := s.DoDeleteService(ctx, mapCloseRequest.Header)
		if err != nil {
			return nil, err
		}

		logHeader, err := s.DoDeleteService(ctx, logCloseRequest.Header)
		if err != nil {
			return nil, err
		}

		response := &api.CloseResponse{
			Header: logHeader,
		}
		log.Tracef("Sending CloseResponse %+v", response)
		return response, nil
	}

	logHeader, err := s.DoCloseService(ctx, logCloseRequest.Header)
	if err != nil {
		return nil, err
	}

	_, err = s.DoCloseService(ctx, mapCloseRequest.Header)
	if err != nil {
		return nil, err
	}

	// Use the log header in creating close response
	response := &api.CloseResponse{
		Header: logHeader,
	}
	log.Tracef("Sending CloseResponse %+v", response)
	return response, nil
}

// Size gets the number of entries in the map
func (s *Server) Size(ctx context.Context, request *api.SizeRequest) (*api.SizeResponse, error) {
	log.Tracef("Received SizeRequest %+v", request)
	// Use log primitive to retrieve the size of map
	logSizeRequest := logapi.SizeRequest{}
	in, err := proto.Marshal(&logSizeRequest)
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoQuery(ctx, opSize, in, logSizeRequest.Header)
	if err != nil {
		return nil, err
	}

	sizeResponse := &logapi.SizeResponse{}
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
func (s *Server) Exists(ctx context.Context, request *api.ExistsRequest) (*api.ExistsResponse, error) {
	log.Tracef("Received ExistsRequest %+v", request)
	mapExistRequest := mapapi.ExistsRequest{
		Key: request.Key,
	}
	in, err := proto.Marshal(&mapExistRequest)
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoQuery(ctx, opExists, in, mapExistRequest.Header)
	if err != nil {
		return nil, err
	}

	existResponse := &mapapi.ExistsResponse{}
	if err = proto.Unmarshal(out, existResponse); err != nil {
		return nil, err
	}

	response := &api.ExistsResponse{
		Header:      header,
		ContainsKey: existResponse.ContainsKey,
	}
	log.Tracef("Sending ExistsResponse %+v", response)
	return response, nil
}

// Put puts a key/value pair into the map
func (s *Server) Put(ctx context.Context, request *api.PutRequest) (*api.PutResponse, error) {
	log.Tracef("Received PutRequest %+v", request)
	// Creates a log entry value (key + value)
	logValue := LogEntryValue{
		Key:   request.GetKey(),
		Value: request.GetValue(),
	}

	var logAppendRequestValue []byte
	_, err := logValue.MarshalTo(logAppendRequestValue)
	if err != nil {
		return nil, err
	}
	// Add log entry value to the log primitive
	logAppendRequest := logapi.AppendRequest{
		Value: logAppendRequestValue,
	}
	in, err := proto.Marshal(&logAppendRequest)
	if err != nil {
		return nil, err
	}

	out, logHeader, err := s.DoCommand(ctx, opAppend, in, logAppendRequest.Header)
	if err != nil {
		return nil, err
	}

	logAppendResponse := &logapi.AppendResponse{}
	if err = proto.Unmarshal(out, logAppendResponse); err != nil {
		return nil, err
	}

	mapValue := MapValue{
		Index: uint64(logAppendResponse.GetIndex()),
		Value: request.GetValue(),
	}

	var mapPutRequestValue []byte
	_, err = mapValue.MarshalTo(mapPutRequestValue)
	if err != nil {
		return nil, err
	}
	// Add values to the map primitive
	mapPutRequest := mapapi.PutRequest{
		Key:     request.Key,
		Value:   mapPutRequestValue,
		TTL:     request.TTL,
		Version: request.Version,
	}
	in, err = proto.Marshal(&mapPutRequest)
	if err != nil {
		return nil, err
	}

	out, _, err = s.DoCommand(ctx, opPut, in, mapPutRequest.Header)
	if err != nil {
		return nil, err
	}
	mapPutResponse := &mapapi.PutResponse{}
	if err = proto.Unmarshal(out, mapPutResponse); err != nil {
		return nil, err
	}

	response := &api.PutResponse{
		Header:          logHeader,
		Status:          api.ResponseStatus(mapPutResponse.GetStatus()),
		Index:           int64(logAppendResponse.GetIndex()),
		Created:         mapPutResponse.Created,
		Updated:         mapPutResponse.Updated,
		PreviousValue:   mapPutResponse.PreviousValue,
		PreviousVersion: mapPutResponse.PreviousVersion,
	}
	log.Tracef("Sending PutResponse %+v", response)
	return response, nil
}

// Replace replaces a key/value pair in the map
func (s *Server) Replace(ctx context.Context, request *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	log.Tracef("Received ReplaceRequest %+v", request)
	mapReplaceRequest := mapapi.ReplaceRequest{
		Key:             request.Key,
		PreviousValue:   request.PreviousValue,
		PreviousVersion: request.PreviousVersion,
		NewValue:        request.NewValue,
		TTL:             request.TTL,
	}
	in, err := proto.Marshal(&mapReplaceRequest)
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoCommand(ctx, opReplace, in, mapReplaceRequest.Header)
	if err != nil {
		return nil, err
	}

	mapReplaceResponse := &mapapi.ReplaceResponse{}
	if err = proto.Unmarshal(out, mapReplaceResponse); err != nil {
		return nil, err
	}

	response := &api.ReplaceResponse{
		Header: header,
		Status: api.ResponseStatus(mapReplaceResponse.GetStatus()),
		//Index:           int64(replaceResponse.Index),
		Key:             request.GetKey(),
		Created:         mapReplaceResponse.Created,
		Updated:         mapReplaceResponse.Updated,
		PreviousValue:   mapReplaceResponse.PreviousValue,
		PreviousVersion: int64(mapReplaceResponse.PreviousVersion),
	}
	log.Tracef("Sending ReplaceResponse %+v", response)
	return response, nil
}

// Get gets the value of a key
func (s *Server) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Tracef("Received GetRequest %+v", request)
	mapGetRequest := mapapi.GetRequest{
		Key: request.Key,
	}
	in, err := proto.Marshal(&mapGetRequest)
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoQuery(ctx, opGet, in, mapGetRequest.Header)
	if err != nil {
		return nil, err
	}

	getResponse := &mapapi.GetResponse{}
	if err = proto.Unmarshal(out, getResponse); err != nil {
		return nil, err
	}

	var mapValue MapValue
	if err = proto.Unmarshal(getResponse.Value, &mapValue); err != nil {
		return nil, err
	}
	response := &api.GetResponse{
		Header:  header,
		Index:   int64(mapValue.GetIndex()),
		Key:     request.GetKey(),
		Value:   mapValue.GetValue(),
		Version: int64(getResponse.Version),
		Created: getResponse.Created,
		Updated: getResponse.Updated,
	}
	log.Tracef("Sending GetResponse %+v", response)
	return response, nil
}

// FirstEntry gets the first entry in the map
func (s *Server) FirstEntry(ctx context.Context, request *api.FirstEntryRequest) (*api.FirstEntryResponse, error) {
	log.Tracef("Received FirstEntryRequest %+v", request)
	// Get the first entry from the log primitive
	firstEntryRequest := logapi.FirstEntryRequest{}
	in, err := proto.Marshal(&firstEntryRequest)
	if err != nil {
		return nil, err
	}

	out, logHeader, err := s.DoQuery(ctx, opFirstEntry, in, firstEntryRequest.Header)
	if err != nil {
		return nil, err
	}

	firstEntryResponse := &logapi.FirstEntryResponse{}
	if err = proto.Unmarshal(out, firstEntryResponse); err != nil {
		return nil, err
	}
	logEntryValue := LogEntryValue{}
	if err = proto.Unmarshal(firstEntryResponse.Value, &logEntryValue); err != nil {
		return nil, err
	}

	// Get the first entry value from the Map primitive after retrieving the key from the log
	mapGetRequest := mapapi.GetRequest{
		Key: logEntryValue.Key,
	}

	in, err = proto.Marshal(&mapGetRequest)
	if err != nil {
		return nil, err
	}

	out, _, err = s.DoQuery(ctx, opGet, in, mapGetRequest.Header)
	if err != nil {
		return nil, err
	}

	mapGetResponse := mapapi.GetResponse{}
	if err = proto.Unmarshal(out, &mapGetResponse); err != nil {
		return nil, err
	}
	var mapValue MapValue
	if err = proto.Unmarshal(mapGetResponse.Value, &mapValue); err != nil {
		return nil, err
	}
	response := &api.FirstEntryResponse{
		Header:  logHeader,
		Index:   int64(mapValue.GetIndex()),
		Key:     logEntryValue.Key,
		Value:   mapValue.GetValue(),
		Version: int64(mapGetResponse.Version),
		Created: mapGetResponse.Created,
		Updated: mapGetResponse.Updated,
	}
	log.Tracef("Sending FirstEntryResponse %+v", response)
	return response, nil
}

// LastEntry gets the last entry in the map
func (s *Server) LastEntry(ctx context.Context, request *api.LastEntryRequest) (*api.LastEntryResponse, error) {
	log.Tracef("Received LastEntryRequest %+v", request)
	lastEntry := logapi.LastEntryRequest{}
	in, err := proto.Marshal(&lastEntry)
	if err != nil {
		return nil, err
	}

	out, logHeader, err := s.DoQuery(ctx, opLastEntry, in, lastEntry.Header)
	if err != nil {
		return nil, err
	}

	lastEntryResponse := &logapi.LastEntryResponse{}
	if err = proto.Unmarshal(out, lastEntryResponse); err != nil {
		return nil, err
	}

	logEntryValue := LogEntryValue{}
	if err = proto.Unmarshal(lastEntryResponse.Value, &logEntryValue); err != nil {
		return nil, err
	}

	// Get the last entry from the map
	mapGetRequest := mapapi.GetRequest{
		Key: logEntryValue.Key,
	}

	in, err = proto.Marshal(&mapGetRequest)
	if err != nil {
		return nil, err
	}

	out, _, err = s.DoQuery(ctx, opGet, in, mapGetRequest.Header)
	if err != nil {
		return nil, err
	}

	mapGetResponse := mapapi.GetResponse{}
	if err = proto.Unmarshal(out, &mapGetResponse); err != nil {
		return nil, err
	}
	var mapValue MapValue
	if err = proto.Unmarshal(mapGetResponse.Value, &mapValue); err != nil {
		return nil, err
	}

	response := &api.LastEntryResponse{
		Header:  logHeader,
		Index:   int64(mapValue.GetIndex()),
		Key:     logEntryValue.Key,
		Value:   mapValue.GetValue(),
		Version: int64(mapGetResponse.Version),
		Created: mapGetResponse.Created,
		Updated: mapGetResponse.Updated,
	}
	log.Tracef("Sending LastEntryResponse %+v", response)
	return response, nil
}

// PrevEntry gets the previous entry in the map
func (s *Server) PrevEntry(ctx context.Context, request *api.PrevEntryRequest) (*api.PrevEntryResponse, error) {
	log.Tracef("Received PrevEntryRequest %+v", request)
	prevEntryRequest := logapi.PrevEntryRequest{
		Index: request.Index,
	}
	in, err := proto.Marshal(&prevEntryRequest)
	if err != nil {
		return nil, err
	}

	out, logHeader, err := s.DoQuery(ctx, opPrevEntry, in, prevEntryRequest.Header)
	if err != nil {
		return nil, err
	}

	prevEntryResponse := &logapi.PrevEntryResponse{}
	if err = proto.Unmarshal(out, prevEntryResponse); err != nil {
		return nil, err
	}

	logEntryValue := LogEntryValue{}
	if err = proto.Unmarshal(prevEntryResponse.Value, &logEntryValue); err != nil {
		return nil, err
	}

	// Get the last entry from the map
	mapGetRequest := mapapi.GetRequest{
		Key: logEntryValue.Key,
	}

	in, err = proto.Marshal(&mapGetRequest)
	if err != nil {
		return nil, err
	}

	out, _, err = s.DoQuery(ctx, opGet, in, mapGetRequest.Header)
	if err != nil {
		return nil, err
	}

	mapGetResponse := mapapi.GetResponse{}
	if err = proto.Unmarshal(out, &mapGetResponse); err != nil {
		return nil, err
	}

	var mapValue MapValue
	if err = proto.Unmarshal(mapGetResponse.Value, &mapValue); err != nil {
		return nil, err
	}

	response := &api.PrevEntryResponse{
		Header:  logHeader,
		Index:   int64(mapValue.GetIndex()),
		Key:     logEntryValue.Key,
		Value:   mapValue.GetValue(),
		Version: int64(mapGetResponse.Version),
		Created: mapGetResponse.Created,
		Updated: mapGetResponse.Updated,
	}
	log.Tracef("Sending PrevEntryResponse %+v", response)
	return response, nil
}

// NextEntry gets the next entry in the map
func (s *Server) NextEntry(ctx context.Context, request *api.NextEntryRequest) (*api.NextEntryResponse, error) {
	log.Tracef("Received NextEntryRequest %+v", request)
	nextEntryRequest := logapi.NextEntryRequest{
		Index: request.Index,
	}
	in, err := proto.Marshal(&nextEntryRequest)
	if err != nil {
		return nil, err
	}

	out, logHeader, err := s.DoQuery(ctx, opNextEntry, in, nextEntryRequest.Header)
	if err != nil {
		return nil, err
	}

	nextEntryResponse := &logapi.NextEntryResponse{}
	if err = proto.Unmarshal(out, nextEntryResponse); err != nil {
		return nil, err
	}

	logEntryValue := LogEntryValue{}
	if err = proto.Unmarshal(nextEntryResponse.Value, &logEntryValue); err != nil {
		return nil, err
	}

	// Get the last entry from the map
	mapGetRequest := mapapi.GetRequest{
		Key: logEntryValue.Key,
	}

	in, err = proto.Marshal(&mapGetRequest)
	if err != nil {
		return nil, err
	}

	out, _, err = s.DoQuery(ctx, opGet, in, mapGetRequest.Header)
	if err != nil {
		return nil, err
	}

	mapGetResponse := mapapi.GetResponse{}
	if err = proto.Unmarshal(out, &mapGetResponse); err != nil {
		return nil, err
	}

	var mapValue MapValue
	if err = proto.Unmarshal(mapGetResponse.Value, &mapValue); err != nil {
		return nil, err
	}

	response := &api.NextEntryResponse{
		Header:  logHeader,
		Index:   int64(mapValue.GetIndex()),
		Key:     logEntryValue.Key,
		Value:   mapValue.GetValue(),
		Version: int64(mapGetResponse.Version),
		Created: mapGetResponse.Created,
		Updated: mapGetResponse.Updated,
	}
	log.Tracef("Sending NextEntryResponse %+v", response)
	return response, nil
}

// Remove removes a key from the map
func (s *Server) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	log.Tracef("Received RemoveRequest %+v", request)
	removeRequest := mapapi.RemoveRequest{
		Key:     request.Key,
		Version: request.Version,
	}
	in, err := proto.Marshal(&removeRequest)
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoCommand(ctx, opRemove, in, removeRequest.Header)
	if err != nil {
		return nil, err
	}

	removeResponse := &mapapi.RemoveResponse{}
	if err = proto.Unmarshal(out, removeResponse); err != nil {
		return nil, err
	}
	var previousValue MapValue
	if err = proto.Unmarshal(removeResponse.PreviousValue, &previousValue); err != nil {
		return nil, err
	}

	response := &api.RemoveResponse{
		Header:          header,
		Status:          api.ResponseStatus(removeResponse.Status),
		Index:           int64(previousValue.GetIndex()),
		Key:             request.Key,
		PreviousValue:   previousValue.GetValue(),
		PreviousVersion: int64(removeResponse.PreviousVersion),
	}
	log.Tracef("Sending RemoveRequest %+v", response)
	return response, nil
}

// Clear removes all keys from the map
func (s *Server) Clear(ctx context.Context, request *api.ClearRequest) (*api.ClearResponse, error) {
	log.Tracef("Received ClearRequest %+v", request)
	mapClearRequest := mapapi.ClearRequest{}
	logClearRequest := logapi.ClearRequest{}

	in, err := proto.Marshal(&logClearRequest)
	if err != nil {
		return nil, err
	}

	out, logHeader, err := s.DoCommand(ctx, opClear, in, logClearRequest.Header)
	if err != nil {
		return nil, err
	}

	in, err = proto.Marshal(&mapClearRequest)
	if err != nil {
		return nil, err
	}

	out, _, err = s.DoCommand(ctx, opClear, in, mapClearRequest.Header)
	if err != nil {
		return nil, err
	}

	serviceResponse := &mapapi.ClearResponse{}
	if err = proto.Unmarshal(out, serviceResponse); err != nil {
		return nil, err
	}

	response := &api.ClearResponse{
		Header: logHeader,
	}
	log.Tracef("Sending ClearResponse %+v", response)
	return response, nil
}

// Events listens for map change events
func (s *Server) Events(request *api.EventRequest, srv api.IndexedMapService_EventsServer) error {
	log.Tracef("Received EventRequest %+v", request)
	eventRequest := mapapi.EventRequest{
		Replay: request.Replay,
		Key:    request.Key,
	}
	in, err := proto.Marshal(&eventRequest)
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	if err := s.DoCommandStream(srv.Context(), opEvents, in, eventRequest.Header, stream); err != nil {
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

		response := &mapapi.EventResponse{}
		output := result.Value.(server.SessionOutput)
		if err = proto.Unmarshal(output.Value.([]byte), response); err != nil {
			return err
		}

		var eventResponse *api.EventResponse
		switch output.Header.Type {
		case headers.ResponseType_OPEN_STREAM:
			eventResponse = &api.EventResponse{
				Header: output.Header,
			}
		case headers.ResponseType_CLOSE_STREAM:
			eventResponse = &api.EventResponse{
				Header: output.Header,
			}
		default:
			eventResponse = &api.EventResponse{
				Header:  output.Header,
				Type:    api.EventResponse_Type(response.Type),
				Key:     response.Key,
				Value:   response.Value,
				Version: int64(response.Version),
				Created: response.Created,
				Updated: response.Updated,
			}
		}

		log.Tracef("Sending EventResponse %+v", eventResponse)
		if err = srv.Send(eventResponse); err != nil {
			return err
		}
	}
	log.Tracef("Finished EventRequest %+v", request)
	return nil
}

// Entries lists all entries currently in the map
func (s *Server) Entries(request *api.EntriesRequest, srv api.IndexedMapService_EntriesServer) error {
	log.Tracef("Received EntriesRequest %+v", request)
	entriesRequest := mapapi.EntriesRequest{}
	in, err := proto.Marshal(&entriesRequest)
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	if err := s.DoQueryStream(srv.Context(), opEntries, in, entriesRequest.Header, stream); err != nil {
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

		response := &mapapi.EntriesResponse{}
		output := result.Value.(server.SessionOutput)
		if err = proto.Unmarshal(output.Value.([]byte), response); err != nil {
			return err
		}

		var entriesResponse *api.EntriesResponse
		switch output.Header.Type {
		case headers.ResponseType_OPEN_STREAM:
			entriesResponse = &api.EntriesResponse{
				Header: output.Header,
			}
		case headers.ResponseType_CLOSE_STREAM:
			entriesResponse = &api.EntriesResponse{
				Header: output.Header,
			}
		default:
			entriesResponse = &api.EntriesResponse{
				Header:  output.Header,
				Key:     response.Key,
				Value:   response.Value,
				Version: int64(response.Version),
				Created: response.Created,
				Updated: response.Updated,
			}
		}

		log.Tracef("Sending EntriesResponse %+v", entriesResponse)
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
	}
	return api.EventResponse_NONE
}
