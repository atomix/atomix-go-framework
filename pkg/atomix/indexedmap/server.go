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
	"github.com/atomix/api/go/atomix/storage"
	api "github.com/atomix/api/go/atomix/storage/indexedmap"
	"github.com/atomix/api/go/atomix/storage/timestamp"
	"github.com/atomix/go-framework/pkg/atomix/primitive"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

// Server is an implementation of MapServiceServer for the map primitive
type Server struct {
	api.IndexedMapServiceServer
	*primitive.Server
}

// Create opens a new session
func (s *Server) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	log.Tracef("Received CreateRequest %+v", request)
	header, err := s.DoCreateService(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	response := &api.CreateResponse{
		Header: *header,
	}
	log.Tracef("Sending CreateResponse %+v", response)
	return response, nil
}

// Close closes a session
func (s *Server) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	log.Tracef("Received CloseRequest %+v", request)
	if request.Delete {
		header, err := s.DoDeleteService(ctx, request.Header)
		if err != nil {
			return nil, err
		}
		response := &api.CloseResponse{
			Header: *header,
		}
		log.Tracef("Sending CloseResponse %+v", response)
		return response, nil
	}

	header, err := s.DoCloseService(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	response := &api.CloseResponse{
		Header: *header,
	}
	log.Tracef("Sending CloseResponse %+v", response)
	return response, nil
}

// Size gets the number of entries in the map
func (s *Server) Size(ctx context.Context, request *api.SizeRequest) (*api.SizeResponse, error) {
	log.Tracef("Received SizeRequest %+v", request)
	in, err := proto.Marshal(&SizeRequest{})
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoQuery(ctx, opSize, in, request.Header)
	if err != nil {
		return nil, err
	}

	sizeResponse := &SizeResponse{}
	if err = proto.Unmarshal(out, sizeResponse); err != nil {
		return nil, err
	}

	response := &api.SizeResponse{
		Header: *header,
		Size_:  sizeResponse.Size_,
	}
	log.Tracef("Sending SizeResponse %+v", response)
	return response, nil
}

// Exists checks whether the map contains a key
func (s *Server) Exists(ctx context.Context, request *api.ExistsRequest) (*api.ExistsResponse, error) {
	log.Tracef("Received ExistsRequest %+v", request)
	in, err := proto.Marshal(&ContainsKeyRequest{
		Key: request.Key,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoQuery(ctx, opExists, in, request.Header)
	if err != nil {
		return nil, err
	}

	containsResponse := &ContainsKeyResponse{}
	if err = proto.Unmarshal(out, containsResponse); err != nil {
		return nil, err
	}

	response := &api.ExistsResponse{
		Header:      *header,
		ContainsKey: containsResponse.ContainsKey,
	}
	log.Tracef("Sending ExistsResponse %+v", response)
	return response, nil
}

// Put puts a key/value pair into the map
func (s *Server) Put(ctx context.Context, request *api.PutRequest) (*api.PutResponse, error) {
	log.Tracef("Received PutRequest %+v", request)
	in, err := proto.Marshal(&PutRequest{
		Index:   request.Index,
		Key:     request.Key,
		Value:   request.Value,
		Version: request.Version,
		TTL:     request.TTL,
		IfEmpty: request.IfEmpty,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoCommand(ctx, opPut, in, request.Header)
	if err != nil {
		return nil, err
	}

	putResponse := &PutResponse{}
	if err = proto.Unmarshal(out, putResponse); err != nil {
		return nil, err
	}

	response := &api.PutResponse{
		Header: *header,
		Entry: &api.Entry{
			Key:   request.Key,
			Value: putResponse.PreviousValue,
			Index: putResponse.Index,
			Timestamp: timestamp.Timestamp{
				Timestamp: &timestamp.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &timestamp.LogicalTimestamp{
						Value: putResponse.PreviousVersion,
					},
				},
			},
			Created: putResponse.Created,
			Updated: putResponse.Updated,
		},
	}
	log.Tracef("Sending PutResponse %+v", response)
	return response, nil
}

// Get gets the value of a key
func (s *Server) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	log.Tracef("Received GetRequest %+v", request)
	in, err := proto.Marshal(&GetRequest{
		Index: request.Index,
		Key:   request.Key,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoQuery(ctx, opGet, in, request.Header)
	if err != nil {
		return nil, err
	}

	getResponse := &GetResponse{}
	if err = proto.Unmarshal(out, getResponse); err != nil {
		return nil, err
	}

	response := &api.GetResponse{
		Header: *header,
		Entry: &api.Entry{
			Key:   request.Key,
			Value: getResponse.Value,
			Index: getResponse.Index,
			Timestamp: timestamp.Timestamp{
				Timestamp: &timestamp.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &timestamp.LogicalTimestamp{
						Value: getResponse.Version,
					},
				},
			},
			Created: getResponse.Created,
			Updated: getResponse.Updated,
		},
	}
	log.Tracef("Sending GetResponse %+v", response)
	return response, nil
}

// FirstEntry gets the first entry in the map
func (s *Server) FirstEntry(ctx context.Context, request *api.FirstEntryRequest) (*api.FirstEntryResponse, error) {
	log.Tracef("Received FirstEntryRequest %+v", request)
	in, err := proto.Marshal(&FirstEntryRequest{})
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoQuery(ctx, opFirstEntry, in, request.Header)
	if err != nil {
		return nil, err
	}

	firstEntryResponse := &FirstEntryResponse{}
	if err = proto.Unmarshal(out, firstEntryResponse); err != nil {
		return nil, err
	}

	response := &api.FirstEntryResponse{
		Header: *header,
		Entry: &api.Entry{
			Key:   firstEntryResponse.Key,
			Value: firstEntryResponse.Value,
			Index: firstEntryResponse.Index,
			Timestamp: timestamp.Timestamp{
				Timestamp: &timestamp.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &timestamp.LogicalTimestamp{
						Value: firstEntryResponse.Version,
					},
				},
			},
			Created: firstEntryResponse.Created,
			Updated: firstEntryResponse.Updated,
		},
	}
	log.Tracef("Sending FirstEntryResponse %+v", response)
	return response, nil
}

// LastEntry gets the last entry in the map
func (s *Server) LastEntry(ctx context.Context, request *api.LastEntryRequest) (*api.LastEntryResponse, error) {
	log.Tracef("Received LastEntryRequest %+v", request)
	in, err := proto.Marshal(&LastEntryRequest{})
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoQuery(ctx, opLastEntry, in, request.Header)
	if err != nil {
		return nil, err
	}

	lastEntryResponse := &LastEntryResponse{}
	if err = proto.Unmarshal(out, lastEntryResponse); err != nil {
		return nil, err
	}

	response := &api.LastEntryResponse{
		Header: *header,
		Entry: &api.Entry{
			Key:   lastEntryResponse.Key,
			Value: lastEntryResponse.Value,
			Index: lastEntryResponse.Index,
			Timestamp: timestamp.Timestamp{
				Timestamp: &timestamp.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &timestamp.LogicalTimestamp{
						Value: lastEntryResponse.Version,
					},
				},
			},
			Created: lastEntryResponse.Created,
			Updated: lastEntryResponse.Updated,
		},
	}
	log.Tracef("Sending LastEntryResponse %+v", response)
	return response, nil
}

// PrevEntry gets the previous entry in the map
func (s *Server) PrevEntry(ctx context.Context, request *api.PrevEntryRequest) (*api.PrevEntryResponse, error) {
	log.Tracef("Received PrevEntryRequest %+v", request)
	in, err := proto.Marshal(&PrevEntryRequest{
		Index: request.Index,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoQuery(ctx, opPrevEntry, in, request.Header)
	if err != nil {
		return nil, err
	}

	prevEntryResponse := &PrevEntryResponse{}
	if err = proto.Unmarshal(out, prevEntryResponse); err != nil {
		return nil, err
	}

	response := &api.PrevEntryResponse{
		Header: *header,
		Entry: &api.Entry{
			Key:   prevEntryResponse.Key,
			Value: prevEntryResponse.Value,
			Index: prevEntryResponse.Index,
			Timestamp: timestamp.Timestamp{
				Timestamp: &timestamp.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &timestamp.LogicalTimestamp{
						Value: prevEntryResponse.Version,
					},
				},
			},
			Created: prevEntryResponse.Created,
			Updated: prevEntryResponse.Updated,
		},
	}
	log.Tracef("Sending PrevEntryResponse %+v", response)
	return response, nil
}

// NextEntry gets the next entry in the map
func (s *Server) NextEntry(ctx context.Context, request *api.NextEntryRequest) (*api.NextEntryResponse, error) {
	log.Tracef("Received NextEntryRequest %+v", request)
	in, err := proto.Marshal(&NextEntryRequest{
		Index: request.Index,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoQuery(ctx, opNextEntry, in, request.Header)
	if err != nil {
		return nil, err
	}

	nextEntryResponse := &NextEntryResponse{}
	if err = proto.Unmarshal(out, nextEntryResponse); err != nil {
		return nil, err
	}

	response := &api.NextEntryResponse{
		Header: *header,
		Entry: &api.Entry{
			Key:   nextEntryResponse.Key,
			Value: nextEntryResponse.Value,
			Index: nextEntryResponse.Index,
			Timestamp: timestamp.Timestamp{
				Timestamp: &timestamp.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &timestamp.LogicalTimestamp{
						Value: nextEntryResponse.Version,
					},
				},
			},
			Created: nextEntryResponse.Created,
			Updated: nextEntryResponse.Updated,
		},
	}
	log.Tracef("Sending NextEntryResponse %+v", response)
	return response, nil
}

// Remove removes a key from the map
func (s *Server) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	log.Tracef("Received RemoveRequest %+v", request)
	var version uint64
	if request.Timestamp != nil {
		version = request.Timestamp.GetLogicalTimestamp().Value
	}
	in, err := proto.Marshal(&RemoveRequest{
		Index:   request.Index,
		Key:     request.Key,
		Version: version,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoCommand(ctx, opRemove, in, request.Header)
	if err != nil {
		return nil, err
	}

	removeResponse := &RemoveResponse{}
	if err = proto.Unmarshal(out, removeResponse); err != nil {
		return nil, err
	}

	response := &api.RemoveResponse{
		Header: *header,
		Entry: &api.Entry{
			Key:   request.Key,
			Value: removeResponse.PreviousValue,
			Index: removeResponse.Index,
			Timestamp: timestamp.Timestamp{
				Timestamp: &timestamp.Timestamp_LogicalTimestamp{
					LogicalTimestamp: &timestamp.LogicalTimestamp{
						Value: removeResponse.PreviousVersion,
					},
				},
			},
			Created: removeResponse.Created,
			Updated: removeResponse.Updated,
		},
	}
	log.Tracef("Sending RemoveRequest %+v", response)
	return response, nil
}

// Clear removes all keys from the map
func (s *Server) Clear(ctx context.Context, request *api.ClearRequest) (*api.ClearResponse, error) {
	log.Tracef("Received ClearRequest %+v", request)
	in, err := proto.Marshal(&ClearRequest{})
	if err != nil {
		return nil, err
	}

	out, header, err := s.DoCommand(ctx, opClear, in, request.Header)
	if err != nil {
		return nil, err
	}

	serviceResponse := &ClearResponse{}
	if err = proto.Unmarshal(out, serviceResponse); err != nil {
		return nil, err
	}

	response := &api.ClearResponse{
		Header: *header,
	}
	log.Tracef("Sending ClearResponse %+v", response)
	return response, nil
}

// Events listens for map change events
func (s *Server) Events(request *api.EventRequest, srv api.IndexedMapService_EventsServer) error {
	log.Tracef("Received EventRequest %+v", request)
	in, err := proto.Marshal(&ListenRequest{
		Replay: request.Replay,
		Key:    request.Key,
		Index:  request.Index,
	})
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	if err := s.DoCommandStream(srv.Context(), opEvents, in, request.Header, stream); err != nil {
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
		output := result.Value.(primitive.SessionOutput)
		if err = proto.Unmarshal(output.Value.([]byte), response); err != nil {
			return err
		}

		var eventResponse *api.EventResponse
		switch output.Header.State.Type {
		case storage.ResponseType_OPEN_STREAM:
			eventResponse = &api.EventResponse{
				Header: *output.Header,
			}
		case storage.ResponseType_CLOSE_STREAM:
			eventResponse = &api.EventResponse{
				Header: *output.Header,
			}
		default:
			eventResponse = &api.EventResponse{
				Header: *output.Header,
				Type:   getEventType(response.Type),
				Entry: api.Entry{
					Key:   response.Key,
					Value: response.Value,
					Index: response.Index,
					Timestamp: timestamp.Timestamp{
						Timestamp: &timestamp.Timestamp_LogicalTimestamp{
							LogicalTimestamp: &timestamp.LogicalTimestamp{
								Value: response.Version,
							},
						},
					},
					Created: response.Created,
					Updated: response.Updated,
				},
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
	in, err := proto.Marshal(&EntriesRequest{})
	if err != nil {
		return err
	}

	stream := streams.NewBufferedStream()
	if err := s.DoQueryStream(srv.Context(), opEntries, in, request.Header, stream); err != nil {
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
		output := result.Value.(primitive.SessionOutput)
		if err = proto.Unmarshal(output.Value.([]byte), response); err != nil {
			return err
		}

		var entriesResponse *api.EntriesResponse
		switch output.Header.State.Type {
		case storage.ResponseType_OPEN_STREAM:
			entriesResponse = &api.EntriesResponse{
				Header: *output.Header,
			}
		case storage.ResponseType_CLOSE_STREAM:
			entriesResponse = &api.EntriesResponse{
				Header: *output.Header,
			}
		default:
			entriesResponse = &api.EntriesResponse{
				Header: *output.Header,
				Entry: api.Entry{
					Key:   response.Key,
					Value: response.Value,
					Index: response.Index,
					Timestamp: timestamp.Timestamp{
						Timestamp: &timestamp.Timestamp_LogicalTimestamp{
							LogicalTimestamp: &timestamp.LogicalTimestamp{
								Value: response.Version,
							},
						},
					},
					Created: response.Created,
					Updated: response.Updated,
				},
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
