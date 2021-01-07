package _map

import (
	"context"
	_map "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"io"
)

const Type = "Map"

const (
	sizeOp     = "Size"
	existsOp   = "Exists"
	putOp      = "Put"
	getOp      = "Get"
	removeOp   = "Remove"
	clearOp    = "Clear"
	eventsOp   = "Events"
	entriesOp  = "Entries"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.RegisterServer(Type, func(server *grpc.Server, client *rsm.Client) {
		_map.RegisterMapServiceServer(server, &Proxy{
			Proxy: rsm.NewProxy(client),
			log:   logging.GetLogger("atomix", "map"),
		})
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *_map.SizeRequest) (*_map.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)

	var err error
	var inputBytes []byte
	partitions := s.Partitions()
	outputsBytes, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		return partitions[i].DoQuery(ctx, sizeOp, inputBytes, request.Header)
	})
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, err
	}
	outputs := make([]_map.SizeOutput, 0, len(outputsBytes))
	for _, outputBytes := range outputsBytes {
		output := _map.SizeOutput{}
		err = proto.Unmarshal(outputBytes.([]byte), &output)
		if err != nil {
			s.log.Errorf("Request SizeRequest failed: %v", err)
			return nil, err
		}
		outputs = append(outputs, output)
	}

	response := &_map.SizeResponse{}
	for _, o := range outputs {
		response.Output.Size_ += o.Size_
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Proxy) Exists(ctx context.Context, request *_map.ExistsRequest) (*_map.ExistsResponse, error) {
	s.log.Debugf("Received ExistsRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request ExistsRequest failed: %v", err)
		return nil, err
	}
	partitionKey := request.Input.Key
	partition := s.PartitionBy([]byte(partitionKey))
	outputBytes, err := partition.DoQuery(ctx, existsOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request ExistsRequest failed: %v", err)
		return nil, err
	}

	response := &_map.ExistsResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request ExistsRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending ExistsResponse %+v", response)
	return response, nil
}

func (s *Proxy) Put(ctx context.Context, request *_map.PutRequest) (*_map.PutResponse, error) {
	s.log.Debugf("Received PutRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request PutRequest failed: %v", err)
		return nil, err
	}
	partitionKey := request.Input.Key
	partition := s.PartitionBy([]byte(partitionKey))
	outputBytes, err := partition.DoCommand(ctx, putOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request PutRequest failed: %v", err)
		return nil, err
	}

	response := &_map.PutResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request PutRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending PutResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *_map.GetRequest) (*_map.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}
	partitionKey := request.Input.Key
	partition := s.PartitionBy([]byte(partitionKey))
	outputBytes, err := partition.DoQuery(ctx, getOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}

	response := &_map.GetResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) Remove(ctx context.Context, request *_map.RemoveRequest) (*_map.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, err
	}
	partitionKey := request.Input.Key
	partition := s.PartitionBy([]byte(partitionKey))
	outputBytes, err := partition.DoCommand(ctx, removeOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, err
	}

	response := &_map.RemoveResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Proxy) Clear(ctx context.Context, request *_map.ClearRequest) (*_map.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)

	var err error
	var inputBytes []byte
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		_, err := partitions[i].DoCommand(ctx, clearOp, inputBytes, request.Header)
		return err
	})
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, err
	}

	response := &_map.ClearResponse{}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *_map.EventsRequest, srv _map.MapService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCommandStream(srv.Context(), eventsOp, inputBytes, request.Header, stream)
	})
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request EventsRequest failed: %v", result.Error)
			return result.Error
		}

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &_map.EventsResponse{
			Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
		output := &response.Output
		err = proto.Unmarshal(outputBytes, output)
		if err != nil {
			s.log.Errorf("Request EventsRequest failed: %v", err)
			return err
		}

		s.log.Debugf("Sending EventsResponse %+v", response)
		if err = srv.Send(response); err != nil {
			s.log.Errorf("Response EventsResponse failed: %v", err)
			return err
		}
	}
	s.log.Debugf("Finished EventsRequest %+v", request)
	return nil
}

func (s *Proxy) Entries(request *_map.EntriesRequest, srv _map.MapService_EntriesServer) error {
	s.log.Debugf("Received EntriesRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request EntriesRequest failed: %v", err)
		return err
	}

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoQueryStream(srv.Context(), entriesOp, inputBytes, request.Header, stream)
	})
	if err != nil {
		s.log.Errorf("Request EntriesRequest failed: %v", err)
		return err
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request EntriesRequest failed: %v", result.Error)
			return result.Error
		}

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &_map.EntriesResponse{
			Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
		output := &response.Output
		err = proto.Unmarshal(outputBytes, output)
		if err != nil {
			s.log.Errorf("Request EntriesRequest failed: %v", err)
			return err
		}

		s.log.Debugf("Sending EntriesResponse %+v", response)
		if err = srv.Send(response); err != nil {
			s.log.Errorf("Response EntriesResponse failed: %v", err)
			return err
		}
	}
	s.log.Debugf("Finished EntriesRequest %+v", request)
	return nil
}

func (s *Proxy) Snapshot(request *_map.SnapshotRequest, srv _map.MapService_SnapshotServer) error {
	s.log.Debugf("Received SnapshotRequest %+v", request)

	var err error
	var inputBytes []byte

	stream := streams.NewBufferedStream()
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].DoCommandStream(srv.Context(), snapshotOp, inputBytes, request.Header, stream)
	})
	if err != nil {
		s.log.Errorf("Request SnapshotRequest failed: %v", err)
		return err
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request SnapshotRequest failed: %v", result.Error)
			return result.Error
		}

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &_map.SnapshotResponse{
			Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
		output := &response.Entry
		err = proto.Unmarshal(outputBytes, output)
		if err != nil {
			s.log.Errorf("Request SnapshotRequest failed: %v", err)
			return err
		}

		s.log.Debugf("Sending SnapshotResponse %+v", response)
		if err = srv.Send(response); err != nil {
			s.log.Errorf("Response SnapshotResponse failed: %v", err)
			return err
		}
	}
	s.log.Debugf("Finished SnapshotRequest %+v", request)
	return nil
}

func (s *Proxy) Restore(srv _map.MapService_RestoreServer) error {
	response := &_map.RestoreResponse{}
	for {
		request, err := srv.Recv()
		if err == io.EOF {
			s.log.Debugf("Sending RestoreResponse %+v", response)
			return srv.SendAndClose(response)
		} else if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return err
		}

		s.log.Debugf("Received RestoreRequest %+v", request)
		input := &request.Entry
		inputBytes, err := proto.Marshal(input)
		if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return err
		}
		partitions := s.Partitions()
		err = async.IterAsync(len(partitions), func(i int) error {
			_, err := partitions[i].DoCommand(srv.Context(), restoreOp, inputBytes, request.Header)
			return err
		})
		if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return err
		}
	}
}
