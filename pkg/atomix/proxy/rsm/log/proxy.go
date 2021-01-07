package log

import (
	"context"
	log "github.com/atomix/api/go/atomix/primitive/log"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"io"
)

const Type = "Log"

const (
	sizeOp       = "Size"
	existsOp     = "Exists"
	appendOp     = "Append"
	getOp        = "Get"
	firstEntryOp = "FirstEntry"
	lastEntryOp  = "LastEntry"
	prevEntryOp  = "PrevEntry"
	nextEntryOp  = "NextEntry"
	removeOp     = "Remove"
	clearOp      = "Clear"
	eventsOp     = "Events"
	entriesOp    = "Entries"
	snapshotOp   = "Snapshot"
	restoreOp    = "Restore"
)

// RegisterProxy registers the primitive on the given node
func RegisterProxy(node *rsm.Node) {
	node.RegisterServer(Type, func(server *grpc.Server, client *rsm.Client) {
		log.RegisterLogServiceServer(server, &Proxy{
			Proxy: rsm.NewProxy(client),
			log:   logging.GetLogger("atomix", "log"),
		})
	})
}

type Proxy struct {
	*rsm.Proxy
	log logging.Logger
}

func (s *Proxy) Size(ctx context.Context, request *log.SizeRequest) (*log.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)

	var err error
	var inputBytes []byte
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, sizeOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, err
	}

	response := &log.SizeResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request SizeRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}

func (s *Proxy) Exists(ctx context.Context, request *log.ExistsRequest) (*log.ExistsResponse, error) {
	s.log.Debugf("Received ExistsRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request ExistsRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, existsOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request ExistsRequest failed: %v", err)
		return nil, err
	}

	response := &log.ExistsResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request ExistsRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending ExistsResponse %+v", response)
	return response, nil
}

func (s *Proxy) Append(ctx context.Context, request *log.AppendRequest) (*log.AppendResponse, error) {
	s.log.Debugf("Received AppendRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request AppendRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, appendOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request AppendRequest failed: %v", err)
		return nil, err
	}

	response := &log.AppendResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request AppendRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending AppendResponse %+v", response)
	return response, nil
}

func (s *Proxy) Get(ctx context.Context, request *log.GetRequest) (*log.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, getOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}

	response := &log.GetResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request GetRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}

func (s *Proxy) FirstEntry(ctx context.Context, request *log.FirstEntryRequest) (*log.FirstEntryResponse, error) {
	s.log.Debugf("Received FirstEntryRequest %+v", request)

	var err error
	var inputBytes []byte
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, firstEntryOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request FirstEntryRequest failed: %v", err)
		return nil, err
	}

	response := &log.FirstEntryResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request FirstEntryRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending FirstEntryResponse %+v", response)
	return response, nil
}

func (s *Proxy) LastEntry(ctx context.Context, request *log.LastEntryRequest) (*log.LastEntryResponse, error) {
	s.log.Debugf("Received LastEntryRequest %+v", request)

	var err error
	var inputBytes []byte
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, lastEntryOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request LastEntryRequest failed: %v", err)
		return nil, err
	}

	response := &log.LastEntryResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request LastEntryRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending LastEntryResponse %+v", response)
	return response, nil
}

func (s *Proxy) PrevEntry(ctx context.Context, request *log.PrevEntryRequest) (*log.PrevEntryResponse, error) {
	s.log.Debugf("Received PrevEntryRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request PrevEntryRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, prevEntryOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request PrevEntryRequest failed: %v", err)
		return nil, err
	}

	response := &log.PrevEntryResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request PrevEntryRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending PrevEntryResponse %+v", response)
	return response, nil
}

func (s *Proxy) NextEntry(ctx context.Context, request *log.NextEntryRequest) (*log.NextEntryResponse, error) {
	s.log.Debugf("Received NextEntryRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request NextEntryRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoQuery(ctx, nextEntryOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request NextEntryRequest failed: %v", err)
		return nil, err
	}

	response := &log.NextEntryResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request NextEntryRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending NextEntryResponse %+v", response)
	return response, nil
}

func (s *Proxy) Remove(ctx context.Context, request *log.RemoveRequest) (*log.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, err
	}
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	outputBytes, err := partition.DoCommand(ctx, removeOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, err
	}

	response := &log.RemoveResponse{}
	output := &response.Output
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
		s.log.Errorf("Request RemoveRequest failed: %v", err)
		return nil, err
	}
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}

func (s *Proxy) Clear(ctx context.Context, request *log.ClearRequest) (*log.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)

	var err error
	var inputBytes []byte
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)
	_, err = partition.DoCommand(ctx, clearOp, inputBytes, request.Header)
	if err != nil {
		s.log.Errorf("Request ClearRequest failed: %v", err)
		return nil, err
	}

	response := &log.ClearResponse{}
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}

func (s *Proxy) Events(request *log.EventsRequest, srv log.LogService_EventsServer) error {
	s.log.Debugf("Received EventsRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request EventsRequest failed: %v", err)
		return err
	}

	stream := streams.NewBufferedStream()
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	err = partition.DoCommandStream(srv.Context(), eventsOp, inputBytes, request.Header, stream)
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
		response := &log.EventsResponse{
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

func (s *Proxy) Entries(request *log.EntriesRequest, srv log.LogService_EntriesServer) error {
	s.log.Debugf("Received EntriesRequest %+v", request)

	var err error
	input := &request.Input
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		s.log.Errorf("Request EntriesRequest failed: %v", err)
		return err
	}

	stream := streams.NewBufferedStream()
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	err = partition.DoQueryStream(srv.Context(), entriesOp, inputBytes, request.Header, stream)
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
		response := &log.EntriesResponse{
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

func (s *Proxy) Snapshot(request *log.SnapshotRequest, srv log.LogService_SnapshotServer) error {
	s.log.Debugf("Received SnapshotRequest %+v", request)

	var err error
	var inputBytes []byte

	stream := streams.NewBufferedStream()
	header := request.Header
	partition := s.PartitionFor(header.PrimitiveID)

	err = partition.DoCommandStream(srv.Context(), snapshotOp, inputBytes, request.Header, stream)
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
		response := &log.SnapshotResponse{
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

func (s *Proxy) Restore(srv log.LogService_RestoreServer) error {
	response := &log.RestoreResponse{}
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
		header := request.Header
		partition := s.PartitionFor(header.PrimitiveID)
		_, err = partition.DoCommand(srv.Context(), restoreOp, inputBytes, request.Header)
		if err != nil {
			s.log.Errorf("Request RestoreRequest failed: %v", err)
			return err
		}
	}
}
