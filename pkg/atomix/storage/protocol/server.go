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

package protocol

import (
	"context"
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
)

// NewServer creates a new proxy server
func NewServer(cluster cluster.Cluster) *Server {
	return &Server{
		cluster: cluster,
	}
}

// Server is a server for updating the storage configuration
type Server struct {
	cluster cluster.Cluster
}

func (s *Server) UpdateConfig(ctx context.Context, request *protocolapi.UpdateConfigRequest) (*protocolapi.UpdateConfigResponse, error) {
	cluster, ok := s.cluster.(cluster.ConfigurableCluster)
	if !ok {
		return nil, errors.NewNotSupported("protocol does not support configuration changes")
	}
	if err := cluster.Update(request.Config); err != nil {
		return nil, err
	}
	return &protocolapi.UpdateConfigResponse{}, nil
}

var _ protocolapi.ProtocolConfigServiceServer = &Server{}
