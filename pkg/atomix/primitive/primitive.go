package primitive

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	pb "github.com/atomix/atomix-go-node/proto/atomix/primitive"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

// RegisterPrimitiveServer registers the primitive server with the gRPC server
func RegisterPrimitiveServer(server *grpc.Server, client service.Client) {
	pb.RegisterPrimitiveServiceServer(server, newPrimitiveServiceServer(client))
}

// newPrimitiveServer returns a new PrimitiveServiceServer implementation
func newPrimitiveServiceServer(client service.Client) pb.PrimitiveServiceServer {
	return &primitiveServer{
		client: client,
	}
}

// primitiveServer is an implementation of the PrimitiveServiceServer Protobuf service
type primitiveServer struct {
	pb.PrimitiveServiceServer
	client service.Client
}

func (s *primitiveServer) GetPrimitives(ctx context.Context, request *pb.GetPrimitivesRequest) (*pb.GetPrimitivesResponse, error) {
	in, err := proto.Marshal(&service.ServiceRequest{
		Request: &service.ServiceRequest_Metadata{
			Metadata: &service.MetadataRequest{
				Type: request.Type,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	ch := make(chan *service.Result)
	s.client.Read(in, ch)

	result := <- ch
	if result.Failed() {
		return nil, result.Error
	}

	response := &service.ServiceResponse{}
	if err := proto.Unmarshal(result.Output, response); err != nil {
		return nil, err
	}

	metadata := response.GetMetadata()

	primitives := make([]*pb.PrimitiveInfo, len(metadata.Services))
	for i, id := range metadata.Services {
		primitives[i] = &pb.PrimitiveInfo{
			Type: id.Type,
			Name: &pb.Name{
				Name:      id.Name,
				Namespace: id.Namespace,
			},
		}
	}
	return &pb.GetPrimitivesResponse{
		Primitives: primitives,
	}, nil
}
