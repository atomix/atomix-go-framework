// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"context"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
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
