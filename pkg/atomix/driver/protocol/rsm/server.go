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

package rsm

import (
	"context"
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
)

func newServer(node *Node) *Server {
	return &Server{
		node: node,
	}
}

type Server struct {
	node *Node
}

func (s *Server) AddProxy(ctx context.Context, request *driverapi.AddProxyRequest) (*driverapi.AddProxyResponse, error) {
	s.node.log.Debugf("Received AddProxyRequest %+v", request)
	primitiveType, err := s.node.primitives.GetPrimitiveType(request.Proxy.ID.Type)
	if err != nil {
		s.node.log.Warnf("AddProxyRequest %+v failed: %s", request, err)
		return nil, errors.Proto(err)
	}
	err = primitiveType.AddProxy(request.Proxy)
	if err != nil {
		s.node.log.Warnf("AddProxyRequest %+v failed: %s", request, err)
		return nil, errors.Proto(err)
	}
	response := &driverapi.AddProxyResponse{}
	s.node.log.Debugf("Sending AddProxyResponse %+v", response)
	return response, nil
}

func (s *Server) RemoveProxy(ctx context.Context, request *driverapi.RemoveProxyRequest) (*driverapi.RemoveProxyResponse, error) {
	s.node.log.Debugf("Received RemoveProxyRequest %+v", request)
	primitiveType, err := s.node.primitives.GetPrimitiveType(request.ProxyID.Type)
	if err != nil {
		s.node.log.Warnf("RemoveProxyRequest %+v failed: %s", request, err)
		return nil, errors.Proto(err)
	}
	err = primitiveType.RemoveProxy(request.ProxyID)
	if err != nil {
		s.node.log.Warnf("RemoveProxyRequest %+v failed: %s", request, err)
		return nil, errors.Proto(err)
	}
	response := &driverapi.RemoveProxyResponse{}
	s.node.log.Debugf("Sending RemoveProxyResponse %+v", response)
	return response, nil
}

func (s *Server) ConfigureDriver(ctx context.Context, request *driverapi.ConfigureDriverRequest) (*driverapi.ConfigureDriverResponse, error) {
	s.node.log.Debugf("Received ConfigureDriverRequest %+v", request)
	cluster, ok := s.node.Cluster.(cluster.ConfigurableCluster)
	if !ok {
		return nil, errors.Proto(errors.NewNotSupported("protocol does not support configuration changes"))
	}
	replicas := make([]protocolapi.ProtocolReplica, len(request.Driver.Protocol.Replicas))
	for i, replica := range request.Driver.Protocol.Replicas {
		replicas[i] = protocolapi.ProtocolReplica{
			ID:           replica.ID,
			NodeID:       replica.NodeID,
			Host:         replica.Host,
			APIPort:      replica.APIPort,
			ProtocolPort: replica.ProtocolPort,
		}
	}
	partitions := make([]protocolapi.ProtocolPartition, len(request.Driver.Protocol.Partitions))
	for i, partition := range request.Driver.Protocol.Partitions {
		partitions[i] = protocolapi.ProtocolPartition{
			PartitionID: partition.PartitionID,
			Replicas:    partition.Replicas,
		}
	}
	config := protocolapi.ProtocolConfig{
		Replicas:   replicas,
		Partitions: partitions,
	}
	if err := cluster.Update(config); err != nil {
		return nil, err
	}
	response := &driverapi.ConfigureDriverResponse{}
	s.node.log.Debugf("Sending ConfigureDriverResponse %+v", response)
	return response, nil
}

var _ driverapi.ProxyManagementServiceServer = &Server{}
var _ driverapi.DriverManagementServiceServer = &Server{}
