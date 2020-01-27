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

package primitive

import (
	"context"
	api "github.com/atomix/api/proto/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/service"
	"github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	node.RegisterServer(registerServer)
}

// registerServer registers a primitive server with the given gRPC server
func registerServer(server *grpc.Server, protocol node.Protocol) {
	api.RegisterPrimitiveServiceServer(server, newServer(protocol))
}

// newServer returns a new PrimitiveServiceServer implementation
func newServer(protocol node.Protocol) api.PrimitiveServiceServer {
	return &primitiveServer{
		protocol: protocol,
	}
}

// primitiveServer is an implementation of the PrimitiveServiceServer Protobuf service
type primitiveServer struct {
	api.PrimitiveServiceServer
	protocol node.Protocol
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

	primitives := make([]*api.PrimitiveInfo, 0)
	partitions := s.protocol.Partitions()
	for _, partition := range partitions {
		stream := stream.NewUnaryStream()
		if err := partition.Read(ctx, in, stream); err != nil {
			return nil, err
		}

		result, ok := stream.Receive()
		if !ok {
			return nil, status.Error(codes.Internal, "stream closed")
		}
		if result.Failed() {
			return nil, result.Error
		}

		response := &service.ServiceResponse{}
		if err := proto.Unmarshal(result.Value.([]byte), response); err != nil {
			return nil, err
		}

		metadata := response.GetMetadata()
		for i, id := range metadata.Services {
			primitives[i] = &api.PrimitiveInfo{
				Type: id.Type,
				Name: &api.Name{
					Name:      id.Name,
					Namespace: id.Namespace,
				},
			}
		}
	}
	return &api.GetPrimitivesResponse{
		Primitives: primitives,
	}, nil
}
