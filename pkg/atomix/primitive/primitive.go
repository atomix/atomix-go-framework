package primitive

import (
	"context"
	api "github.com/atomix/atomix-api/proto/atomix/primitive"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

// RegisterPrimitiveServer registers the primitive server with the gRPC server
func RegisterPrimitiveServer(server *grpc.Server, client service.Client) {
	api.RegisterPrimitiveServiceServer(server, newPrimitiveServiceServer(client))
}

// newPrimitiveServer returns a new PrimitiveServiceServer implementation
func newPrimitiveServiceServer(client service.Client) api.PrimitiveServiceServer {
	return &primitiveServer{
		client: client,
	}
}

// primitiveServer is an implementation of the PrimitiveServiceServer Protobuf service
type primitiveServer struct {
	api.PrimitiveServiceServer
	client service.Client
}

func (s *primitiveServer) GetPrimitives(ctx context.Context, request *api.GetPrimitivesRequest) (*api.GetPrimitivesResponse, error) {
	in, err := proto.Marshal(&service.ServiceRequest{
		Request: &service.ServiceRequest_Metadata{
			Metadata: &service.MetadataRequest{
				Type:      request.Type,
				Namespace: request.Namespace,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	ch := make(chan service.Output)
	if err := s.client.Read(ctx, in, ch); err != nil {
		return nil, err
	}

	result := <-ch
	if result.Failed() {
		return nil, result.Error
	}

	response := &service.ServiceResponse{}
	if err := proto.Unmarshal(result.Value, response); err != nil {
		return nil, err
	}

	metadata := response.GetMetadata()

	primitives := make([]*api.PrimitiveInfo, len(metadata.Services))
	for i, id := range metadata.Services {
		primitives[i] = &api.PrimitiveInfo{
			Type: id.Type,
			Name: &api.Name{
				Name:      id.Name,
				Namespace: id.Namespace,
			},
		}
	}
	return &api.GetPrimitivesResponse{
		Primitives: primitives,
	}, nil
}
