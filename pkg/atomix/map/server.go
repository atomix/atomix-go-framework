package _map

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/server"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/atomix/atomix-go-node/proto/atomix/headers"
	pb "github.com/atomix/atomix-go-node/proto/atomix/map"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func RegisterMapServer(server *grpc.Server, client service.Client) {
	pb.RegisterMapServiceServer(server, NewMapServiceServer(client))
}

func NewMapServiceServer(client service.Client) pb.MapServiceServer {
	return &mapServer{
		SessionizedServer: &server.SessionizedServer{
			Type:   "map",
			Client: client,
		},
	}
}

// mapServer is an implementation of MapServiceServer for the map primitive
type mapServer struct {
	pb.MapServiceServer
	*server.SessionizedServer
}

func (m *mapServer) Create(ctx context.Context, request *pb.CreateRequest) (*pb.CreateResponse, error) {
	log.Tracef("Received CreateRequest %+v", request)
	session, err := m.OpenSession(ctx, request.Header, request.Timeout)
	if err != nil {
		return nil, err
	}
	response := &pb.CreateResponse{
		Header: &headers.ResponseHeader{
			SessionId: session,
			Index:     session,
		},
	}
	log.Tracef("Sending CreateResponse %+v", response)
	return response, nil
}

func (m *mapServer) KeepAlive(ctx context.Context, request *pb.KeepAliveRequest) (*pb.KeepAliveResponse, error) {
	log.Tracef("Received KeepAliveRequest %+v", request)
	if err := m.KeepAliveSession(ctx, request.Header); err != nil {
		return nil, err
	}
	response := &pb.KeepAliveResponse{
		Header: &headers.ResponseHeader{
			SessionId: request.Header.SessionId,
		},
	}
	log.Tracef("Sending KeepAliveResponse %+v", response)
	return response, nil
}

func (m *mapServer) Close(ctx context.Context, request *pb.CloseRequest) (*pb.CloseResponse, error) {
	log.Tracef("Received CloseRequest %+v", request)
	if err := m.CloseSession(ctx, request.Header); err != nil {
		return nil, err
	}
	response := &pb.CloseResponse{
		Header: &headers.ResponseHeader{
			SessionId: request.Header.SessionId,
		},
	}
	log.Tracef("Sending CloseResponse %+v", response)
	return response, nil
}

func (m *mapServer) Size(ctx context.Context, request *pb.SizeRequest) (*pb.SizeResponse, error) {
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

	response := &pb.SizeResponse{
		Header: header,
		Size:   sizeResponse.Size,
	}
	log.Tracef("Sending SizeResponse %+v", response)
	return response, nil
}

func (m *mapServer) Exists(ctx context.Context, request *pb.ExistsRequest) (*pb.ExistsResponse, error) {
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

	response := &pb.ExistsResponse{
		Header:      header,
		ContainsKey: containsResponse.ContainsKey,
	}
	log.Tracef("Sending ExistsResponse %+v", response)
	return response, nil
}

func (m *mapServer) Put(ctx context.Context, request *pb.PutRequest) (*pb.PutResponse, error) {
	log.Tracef("Received PutRequest %+v", request)
	in, err := proto.Marshal(&PutRequest{
		Key:     request.Key,
		Value:   request.Value,
		Version: uint64(request.Version),
		Ttl:     request.Ttl,
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

	response := &pb.PutResponse{
		Header:          header,
		Status:          getResponseStatus(putResponse.Status),
		PreviousValue:   putResponse.PreviousValue,
		PreviousVersion: int64(putResponse.PreviousVersion),
	}
	log.Tracef("Sending PutResponse %+v", response)
	return response, nil
}

func (m *mapServer) Replace(ctx context.Context, request *pb.ReplaceRequest) (*pb.ReplaceResponse, error) {
	log.Tracef("Received ReplaceRequest %+v", request)
	in, err := proto.Marshal(&ReplaceRequest{
		Key:             request.Key,
		PreviousValue:   request.PreviousValue,
		PreviousVersion: uint64(request.PreviousVersion),
		NewValue:        request.NewValue,
		Ttl:             request.Ttl,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := m.Command(ctx, "replace", in, request.Header)
	if err != nil {
		return nil, err
	}

	serviceResponse := &ReplaceResponse{}
	if err = proto.Unmarshal(out, serviceResponse); err != nil {
		return nil, err
	}

	response := &pb.ReplaceResponse{
		Header:          header,
		Status:          getResponseStatus(serviceResponse.Status),
		PreviousValue:   serviceResponse.PreviousValue,
		PreviousVersion: int64(serviceResponse.PreviousVersion),
	}
	log.Tracef("Sending ReplaceResponse %+v", response)
	return response, nil
}

func (m *mapServer) Get(ctx context.Context, request *pb.GetRequest) (*pb.GetResponse, error) {
	log.Tracef("Received GetRequest %+v", request)
	in, err := proto.Marshal(&GetRequest{
		Key: request.Key,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := m.Query(ctx, "get", in, request.Header)
	if err != nil {
		return nil, err
	}

	serviceResponse := &GetResponse{}
	if err = proto.Unmarshal(out, serviceResponse); err != nil {
		return nil, err
	}

	response := &pb.GetResponse{
		Header:  header,
		Value:   serviceResponse.Value,
		Version: int64(serviceResponse.Version),
	}
	log.Tracef("Sending GetRequest %+v", response)
	return response, nil
}

func (m *mapServer) Remove(ctx context.Context, request *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	log.Tracef("Received RemoveRequest %+v", request)
	in, err := proto.Marshal(&RemoveRequest{
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

	serviceResponse := &RemoveResponse{}
	if err = proto.Unmarshal(out, serviceResponse); err != nil {
		return nil, err
	}

	response := &pb.RemoveResponse{
		Header:          header,
		Status:          getResponseStatus(serviceResponse.Status),
		PreviousValue:   serviceResponse.PreviousValue,
		PreviousVersion: int64(serviceResponse.PreviousVersion),
	}
	log.Tracef("Sending RemoveRequest %+v", response)
	return response, nil
}

func (m *mapServer) Clear(ctx context.Context, request *pb.ClearRequest) (*pb.ClearResponse, error) {
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

	response := &pb.ClearResponse{
		Header: header,
	}
	log.Tracef("Sending ClearResponse %+v", response)
	return response, nil
}

func (m *mapServer) Events(request *pb.EventRequest, srv pb.MapService_EventsServer) error {
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
	} else {
		for result := range ch {
			if result.Failed() {
				return result.Error
			} else {
				response := &ListenResponse{}
				if err = proto.Unmarshal(result.Value, response); err != nil {
					return err
				} else {
					eventResponse := &pb.EventResponse{
						Header:     result.Header,
						Type:       getEventType(response.Type),
						Key:        response.Key,
						OldValue:   response.OldValue,
						OldVersion: int64(response.OldVersion),
						NewValue:   response.NewValue,
						NewVersion: int64(response.NewVersion),
					}
					log.Tracef("Sending EventResponse %+v", response)
					srv.Send(eventResponse)
				}
			}
		}
	}
	log.Tracef("Finished EventRequest %+v", request)
	return nil
}

func (m *mapServer) Entries(request *pb.EntriesRequest, srv pb.MapService_EntriesServer) error {
	log.Tracef("Received EntriesRequest %+v", request)
	in, err := proto.Marshal(&EntriesRequest{})
	if err != nil {
		return err
	}

	ch := make(chan server.SessionOutput)
	if err := m.QueryStream("entries", in, request.Header, ch); err != nil {
		return err
	} else {
		for result := range ch {
			if result.Failed() {
				return result.Error
			} else {
				response := &EntriesResponse{}
				if err = proto.Unmarshal(result.Value, response); err != nil {
					srv.Context().Done()
				} else {
					entriesResponse := &pb.EntriesResponse{
						Header:  result.Header,
						Key:     response.Key,
						Value:   response.Value,
						Version: int64(response.Version),
					}
					log.Tracef("Sending EntriesResponse %+v", response)
					srv.Send(entriesResponse)
				}
			}
		}
	}
	log.Tracef("Finished EntriesRequest %+v", request)
	return nil
}

func getResponseStatus(status UpdateStatus) pb.ResponseStatus {
	switch status {
	case UpdateStatus_OK:
		return pb.ResponseStatus_OK
	case UpdateStatus_NOOP:
		return pb.ResponseStatus_NOOP
	case UpdateStatus_PRECONDITION_FAILED:
		return pb.ResponseStatus_PRECONDITION_FAILED
	case UpdateStatus_WRITE_LOCK:
		return pb.ResponseStatus_WRITE_LOCK
	}
	return pb.ResponseStatus_OK
}

func getEventType(eventType ListenResponse_Type) pb.EventResponse_Type {
	switch eventType {
	case ListenResponse_NONE:
		return pb.EventResponse_NONE
	case ListenResponse_INSERTED:
		return pb.EventResponse_INSERTED
	case ListenResponse_UPDATED:
		return pb.EventResponse_UPDATED
	case ListenResponse_REMOVED:
		return pb.EventResponse_REMOVED
	}
	return pb.EventResponse_UPDATED
}
