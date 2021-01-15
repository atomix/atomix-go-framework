package list

import (
	"context"
	list "github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"io"
)

const Type = "List"

const (
	sizeOp     = "Size"
	appendOp   = "Append"
	insertOp   = "Insert"
	getOp      = "Get"
	setOp      = "Set"
	removeOp   = "Remove"
	clearOp    = "Clear"
	eventsOp   = "Events"
	elementsOp = "Elements"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.RegisterServer(Type, func(server *grpc.Server, client *rsm.Client) {
		list.RegisterListServiceServer(server, &Proxy{
			Proxy: rsm.NewProxy(client),
			log:   logging.GetLogger("atomix", "list"),
		})
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *list.SizeRequest) (*list.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)

	var err error
	var inputBytes []byte
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, sizeOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &list.SizeResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Proxy) Append(ctx context.Context, request *list.AppendRequest) (*list.AppendResponse, error) {
	s.log.Debugf("Received AppendRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request AppendRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, appendOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request AppendRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &list.AppendResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request AppendRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending AppendResponse %+v", response)
	return response, nil
}

func (s *Proxy) Insert(ctx context.Context, request *list.InsertRequest) (*list.InsertResponse, error) {
	s.log.Debugf("Received InsertRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request InsertRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, insertOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request InsertRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &list.InsertResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request InsertRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending InsertResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *list.GetRequest) (*list.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, getOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &list.GetResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Set(ctx context.Context, request *list.SetRequest) (*list.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, setOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &list.SetResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request SetRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Remove(ctx context.Context, request *list.RemoveRequest) (*list.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, removeOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &list.RemoveResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, errors.Proto(err)
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Proxy) Clear(ctx context.Context, request *list.ClearRequest) (*list.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)

	var err error
	var inputBytes []byte
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	_, err = partition.DoCommand(ctx, clearOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, errors.Proto(err)
	}

	response := &list.ClearResponse{}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *list.EventsRequest, srv list.ListService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	err = partition.DoCommandStream(srv.Context(), eventsOp, inputBytes, request.Header, stream)
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

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &list.EventsResponse{
			Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
		output := &response.Output
		err = proto.Unmarshal(outputBytes, output)
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

func (s *Proxy) Elements(request *list.ElementsRequest, srv list.ListService_ElementsServer) error {
	s.log.Debugf("Received ElementsRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request ElementsRequest failed: %v", err)
		return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	err = partition.DoQueryStream(srv.Context(), elementsOp, inputBytes, request.Header, stream)
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

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &list.ElementsResponse{
			Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
		output := &response.Output
		err = proto.Unmarshal(outputBytes, output)
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

func (s *Proxy) Snapshot(request *list.SnapshotRequest, srv list.ListService_SnapshotServer) error {
	s.log.Debugf("Received SnapshotRequest %+v", request)

	var err error
	var inputBytes []byte

	stream := streams.NewBufferedStream()
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	err = partition.DoCommandStream(srv.Context(), snapshotOp, inputBytes, request.Header, stream)
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request SnapshotRequest failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &list.SnapshotResponse{
			Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
		output := &response.Entry
		err = proto.Unmarshal(outputBytes, output)
		if err != nil {
			s.log.Errorf("Request SnapshotRequest failed: %v", err)
			return errors.Proto(err)
		}

		s.log.Debugf("Sending SnapshotResponse %+v", response)
		if err = srv.Send(response); err != nil {
			s.log.Errorf("Response SnapshotResponse failed: %v", err)
			return errors.Proto(err)
		}
	}
	s.log.Debugf("Finished SnapshotRequest %+v", request)
	return nil
}

func (s *Proxy) Restore(srv list.ListService_RestoreServer) error {
	response := &list.RestoreResponse{}
	for {
		request, err := srv.Recv()
		if err == io.EOF {
			s.log.Debugf("Sending RestoreResponse %+v", response)
			return srv.SendAndClose(response)
		} else if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return errors.Proto(err)
		}

		s.log.Debugf("Received RestoreRequest %+v", request)
		input := &request.Entry
		inputBytes, err := proto.Marshal(input)
		if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return errors.Proto(err)
		}
		header := request.Header
		partition := s.PartitionFor(header.PrimitiveID)
		_, err = partition.DoCommand(srv.Context(), restoreOp, inputBytes, request.Header)
		if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return errors.Proto(err)
		}
	}
}
