
package list

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	protocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/golang/protobuf/proto"
	list "github.com/atomix/api/go/atomix/primitive/list"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	
)

const Type = "List"

const (
    sizeOp = "Size"
    appendOp = "Append"
    insertOp = "Insert"
    getOp = "Get"
    setOp = "Set"
    removeOp = "Remove"
    clearOp = "Clear"
    eventsOp = "Events"
    elementsOp = "Elements"
)

// NewListProxyServer creates a new ListProxyServer
func NewListProxyServer(client *rsm.Client) list.ListServiceServer {
	return &ListProxyServer{
		Proxy: rsm.NewProxy(client),
		log:   logging.GetLogger("atomix", "counter"),
	}
}

type ListProxyServer struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *ListProxyServer) Size(ctx context.Context, request *list.SizeRequest) (*list.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request SizeRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition, err := s.PartitionFrom(ctx)
    if err != nil {
        return nil, errors.Proto(err)
    }

	service := protocol.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoQuery(ctx, service, sizeOp, input)
	if err != nil {
        s.log.Errorf("Request SizeRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &list.SizeResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request SizeRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}


func (s *ListProxyServer) Append(ctx context.Context, request *list.AppendRequest) (*list.AppendResponse, error) {
	s.log.Debugf("Received AppendRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request AppendRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition, err := s.PartitionFrom(ctx)
    if err != nil {
        return nil, errors.Proto(err)
    }

	service := protocol.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, appendOp, input)
	if err != nil {
        s.log.Errorf("Request AppendRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &list.AppendResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request AppendRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending AppendResponse %+v", response)
	return response, nil
}


func (s *ListProxyServer) Insert(ctx context.Context, request *list.InsertRequest) (*list.InsertResponse, error) {
	s.log.Debugf("Received InsertRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request InsertRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition, err := s.PartitionFrom(ctx)
    if err != nil {
        return nil, errors.Proto(err)
    }

	service := protocol.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, insertOp, input)
	if err != nil {
        s.log.Errorf("Request InsertRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &list.InsertResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request InsertRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending InsertResponse %+v", response)
	return response, nil
}


func (s *ListProxyServer) Get(ctx context.Context, request *list.GetRequest) (*list.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request GetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition, err := s.PartitionFrom(ctx)
    if err != nil {
        return nil, errors.Proto(err)
    }

	service := protocol.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoQuery(ctx, service, getOp, input)
	if err != nil {
        s.log.Errorf("Request GetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &list.GetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request GetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}


func (s *ListProxyServer) Set(ctx context.Context, request *list.SetRequest) (*list.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request SetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition, err := s.PartitionFrom(ctx)
    if err != nil {
        return nil, errors.Proto(err)
    }

	service := protocol.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, setOp, input)
	if err != nil {
        s.log.Errorf("Request SetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &list.SetResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request SetRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}


func (s *ListProxyServer) Remove(ctx context.Context, request *list.RemoveRequest) (*list.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request RemoveRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition, err := s.PartitionFrom(ctx)
    if err != nil {
        return nil, errors.Proto(err)
    }

	service := protocol.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, removeOp, input)
	if err != nil {
        s.log.Errorf("Request RemoveRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &list.RemoveResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request RemoveRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}


func (s *ListProxyServer) Clear(ctx context.Context, request *list.ClearRequest) (*list.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request ClearRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
    partition, err := s.PartitionFrom(ctx)
    if err != nil {
        return nil, errors.Proto(err)
    }

	service := protocol.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	output, err := partition.DoCommand(ctx, service, clearOp, input)
	if err != nil {
        s.log.Errorf("Request ClearRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &list.ClearResponse{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request ClearRequest failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}


func (s *ListProxyServer) Events(request *list.EventsRequest, srv list.ListService_EventsServer) error {
    s.log.Debugf("Received EventsRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request EventsRequest failed: %v", err)
        return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
    partition, err := s.PartitionFrom(srv.Context())
    if err != nil {
        return errors.Proto(err)
    }

	service := protocol.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	err = partition.DoCommandStream(srv.Context(), service, eventsOp, input, stream)
	if err != nil {
        s.log.Errorf("Request EventsRequest failed: %v", err)
	    return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request EventsRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		response := &list.EventsResponse{}
        err = proto.Unmarshal(result.Value.([]byte), response)
        if err != nil {
            s.log.Errorf("Request EventsRequest failed: %v", err)
            return errors.Proto(err)
        }

		s.log.Debugf("Sending EventsResponse %+v", response)
		if err = srv.Send(response); err != nil {
            s.log.Errorf("Response EventsResponse failed: %v", err)
			return errors.Proto(err)
		}
	}
	s.log.Debugf("Finished EventsRequest %+v", request)
	return nil
}


func (s *ListProxyServer) Elements(request *list.ElementsRequest, srv list.ListService_ElementsServer) error {
    s.log.Debugf("Received ElementsRequest %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request ElementsRequest failed: %v", err)
        return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
    partition, err := s.PartitionFrom(srv.Context())
    if err != nil {
        return errors.Proto(err)
    }

	service := protocol.ServiceId{
		Type:      Type,
		Namespace: request.Headers.PrimitiveID.Namespace,
		Name:      request.Headers.PrimitiveID.Name,
	}
	err = partition.DoQueryStream(srv.Context(), service, elementsOp, input, stream)
	if err != nil {
        s.log.Errorf("Request ElementsRequest failed: %v", err)
	    return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request ElementsRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		response := &list.ElementsResponse{}
        err = proto.Unmarshal(result.Value.([]byte), response)
        if err != nil {
            s.log.Errorf("Request ElementsRequest failed: %v", err)
            return errors.Proto(err)
        }

		s.log.Debugf("Sending ElementsResponse %+v", response)
		if err = srv.Send(response); err != nil {
            s.log.Errorf("Response ElementsResponse failed: %v", err)
			return errors.Proto(err)
		}
	}
	s.log.Debugf("Finished ElementsRequest %+v", request)
	return nil
}

