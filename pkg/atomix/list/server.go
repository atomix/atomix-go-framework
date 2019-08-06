package list

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/server"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/atomix/atomix-go-node/proto/atomix/headers"
	pb "github.com/atomix/atomix-go-node/proto/atomix/list"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func RegisterListServer(server *grpc.Server, client service.Client) {
	pb.RegisterListServiceServer(server, NewListServiceServer(client))
}

func NewListServiceServer(client service.Client) pb.ListServiceServer {
	return &listServer{
		SessionizedServer: &server.SessionizedServer{
			Type:   "list",
			Client: client,
		},
	}
}

// listServer is an implementation of MapServiceServer for the map primitive
type listServer struct {
	*server.SessionizedServer
}

func (s *listServer) Create(ctx context.Context, request *pb.CreateRequest) (*pb.CreateResponse, error) {
	log.Tracef("Received CreateRequest %+v", request)
	session, err := s.OpenSession(ctx, request.Header, request.Timeout)
	if err != nil {
		return nil, err
	}
	response := &pb.CreateResponse{
		Header: &headers.ResponseHeader{
			SessionId: session,
		},
	}
	log.Tracef("Sending CreateResponse %+v", response)
	return response, nil
}

func (s *listServer) KeepAlive(ctx context.Context, request *pb.KeepAliveRequest) (*pb.KeepAliveResponse, error) {
	log.Tracef("Received KeepAliveRequest %+v", request)
	if err := s.KeepAliveSession(ctx, request.Header); err != nil {
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

func (s *listServer) Close(ctx context.Context, request *pb.CloseRequest) (*pb.CloseResponse, error) {
	log.Tracef("Received CloseRequest %+v", request)
	if err := s.CloseSession(ctx, request.Header); err != nil {
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

func (s *listServer) Size(ctx context.Context, request *pb.SizeRequest) (*pb.SizeResponse, error) {
	log.Tracef("Received SizeRequest %+v", request)
	in, err := proto.Marshal(&SizeRequest{})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Query(ctx, "size", in, request.Header)
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

func (s *listServer) Contains(ctx context.Context, request *pb.ContainsRequest) (*pb.ContainsResponse, error) {
	log.Tracef("Received ContainsRequest %+v", request)
	in, err := proto.Marshal(&ContainsRequest{
		Value: request.Value,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Query(ctx, "contains", in, request.Header)
	if err != nil {
		return nil, err
	}

	containsResponse := &ContainsResponse{}
	if err = proto.Unmarshal(out, containsResponse); err != nil {
		return nil, err
	}

	response := &pb.ContainsResponse{
		Header:   header,
		Contains: containsResponse.Contains,
	}
	log.Tracef("Sending ContainsResponse %+v", response)
	return response, nil
}

func (s *listServer) Append(ctx context.Context, request *pb.AppendRequest) (*pb.AppendResponse, error) {
	log.Tracef("Received AppendRequest %+v", request)
	in, err := proto.Marshal(&AppendRequest{
		Value: request.Value,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Command(ctx, "append", in, request.Header)
	if err != nil {
		return nil, err
	}

	appendResponse := &AppendResponse{}
	if err = proto.Unmarshal(out, appendResponse); err != nil {
		return nil, err
	}

	response := &pb.AppendResponse{
		Header: header,
		Status: getResponseStatus(appendResponse.Status),
	}
	log.Tracef("Sending AppendResponse %+v", response)
	return response, nil
}

func (s *listServer) Insert(ctx context.Context, request *pb.InsertRequest) (*pb.InsertResponse, error) {
	log.Tracef("Received InsertRequest %+v", request)
	in, err := proto.Marshal(&InsertRequest{
		Value: request.Value,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Command(ctx, "insert", in, request.Header)
	if err != nil {
		return nil, err
	}

	insertResponse := &InsertResponse{}
	if err = proto.Unmarshal(out, insertResponse); err != nil {
		return nil, err
	}

	response := &pb.InsertResponse{
		Header: header,
		Status: getResponseStatus(insertResponse.Status),
	}
	log.Tracef("Sending InsertResponse %+v", response)
	return response, nil
}

func (s *listServer) Get(ctx context.Context, request *pb.GetRequest) (*pb.GetResponse, error) {
	log.Tracef("Received GetRequest %+v", request)
	in, err := proto.Marshal(&GetRequest{
		Index: request.Index,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Query(ctx, "get", in, request.Header)
	if err != nil {
		return nil, err
	}

	getResponse := &GetResponse{}
	if err = proto.Unmarshal(out, getResponse); err != nil {
		return nil, err
	}

	response := &pb.GetResponse{
		Header: header,
		Status: getResponseStatus(getResponse.Status),
		Value:  getResponse.Value,
	}
	log.Tracef("Sending GetResponse %+v", response)
	return response, nil
}

func (s *listServer) Remove(ctx context.Context, request *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	log.Tracef("Received RemoveRequest %+v", request)
	in, err := proto.Marshal(&RemoveRequest{
		Index: request.Index,
	})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Command(ctx, "remove", in, request.Header)
	if err != nil {
		return nil, err
	}

	removeResponse := &RemoveResponse{}
	if err = proto.Unmarshal(out, removeResponse); err != nil {
		return nil, err
	}

	response := &pb.RemoveResponse{
		Header: header,
		Status: getResponseStatus(removeResponse.Status),
		Value:  removeResponse.Value,
	}
	log.Tracef("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *listServer) Clear(ctx context.Context, request *pb.ClearRequest) (*pb.ClearResponse, error) {
	log.Tracef("Received ClearRequest %+v", request)
	in, err := proto.Marshal(&ClearRequest{})
	if err != nil {
		return nil, err
	}

	out, header, err := s.Command(ctx, "clear", in, request.Header)
	if err != nil {
		return nil, err
	}

	clearResponse := &ClearResponse{}
	if err = proto.Unmarshal(out, clearResponse); err != nil {
		return nil, err
	}

	response := &pb.ClearResponse{
		Header: header,
	}
	log.Tracef("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *listServer) Events(request *pb.EventRequest, srv pb.ListService_EventsServer) error {
	log.Tracef("Received EventRequest %+v", request)
	in, err := proto.Marshal(&ListenRequest{
		Replay: request.Replay,
	})
	if err != nil {
		return err
	}

	ch := make(chan server.SessionOutput)
	if err := s.CommandStream("events", in, request.Header, ch); err != nil {
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
						Header: result.Header,
						Type:   getEventType(response.Type),
						Value:  response.Value,
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

func (s *listServer) Iterate(request *pb.IterateRequest, srv pb.ListService_IterateServer) error {
	log.Tracef("Received IterateRequest %+v", request)
	in, err := proto.Marshal(&IterateRequest{})
	if err != nil {
		return err
	}

	ch := make(chan server.SessionOutput)
	if err := s.QueryStream("iterate", in, request.Header, ch); err != nil {
		return err
	} else {
		for result := range ch {
			if result.Failed() {
				return result.Error
			} else {
				response := &IterateResponse{}
				if err = proto.Unmarshal(result.Value, response); err != nil {
					srv.Context().Done()
				} else {
					iterateResponse := &pb.IterateResponse{
						Header: result.Header,
						Value:  response.Value,
					}
					log.Tracef("Sending IterateResponse %+v", response)
					srv.Send(iterateResponse)
				}
			}
		}
	}
	log.Tracef("Finished IterateRequest %+v", request)
	return nil
}

func getResponseStatus(status ResponseStatus) pb.ResponseStatus {
	switch status {
	case ResponseStatus_OK:
		return pb.ResponseStatus_OK
	case ResponseStatus_NOOP:
		return pb.ResponseStatus_NOOP
	case ResponseStatus_WRITE_LOCK:
		return pb.ResponseStatus_WRITE_LOCK
	}
	return pb.ResponseStatus_OK
}

func getEventType(eventType ListenResponse_Type) pb.EventResponse_Type {
	switch eventType {
	case ListenResponse_NONE:
		return pb.EventResponse_NONE
	case ListenResponse_ADDED:
		return pb.EventResponse_ADDED
	case ListenResponse_REMOVED:
		return pb.EventResponse_REMOVED
	}
	return 0
}
