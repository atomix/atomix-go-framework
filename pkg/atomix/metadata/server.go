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

package metadata

import (
	"context"
	api "github.com/atomix/api/proto/atomix/metadata"
	"github.com/atomix/api/proto/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/server"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func init() {
	node.RegisterServer(registerServer)
}

// registerServer registers a map server with the given gRPC server
func registerServer(server *grpc.Server, protocol node.Protocol) {
	api.RegisterMetadataServiceServer(server, newServer(protocol))
}

func newServer(protocol node.Protocol) api.MetadataServiceServer {
	return &Server{
		Server: &server.Server{
			Protocol: protocol,
		},
	}
}

// Server is an implementation of MapServiceServer for the map primitive
type Server struct {
	*server.Server
}

// GetPrimitives gets a list of primitives in a partition
func (s *Server) GetPrimitives(ctx context.Context, request *api.GetPrimitivesRequest) (*api.GetPrimitivesResponse, error) {
	log.Tracef("Received GetPrimitivesRequest %+v", request)

	services, header, err := s.DoMetadata(ctx, request.Type, request.Namespace, request.Header)
	if err != nil {
		return nil, err
	}

	primitives := make([]*api.PrimitiveMetadata, len(services))
	for i, service := range services {
		primitives[i] = &api.PrimitiveMetadata{
			Type: primitive.PrimitiveType(service.Type),
			Name: &primitive.Name{
				Name:      service.Name,
				Namespace: service.Namespace,
			},
		}
	}

	response := &api.GetPrimitivesResponse{
		Header:     header,
		Primitives: primitives,
	}
	log.Tracef("Sending GetPrimitivesResponse %+v", response)
	return response, nil
}
