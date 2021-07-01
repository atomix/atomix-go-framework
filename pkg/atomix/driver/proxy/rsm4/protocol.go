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
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/server"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
)

// NewProtocol creates a new state machine protocol
func NewProtocol(cluster cluster.Cluster, env env.DriverEnv) *Protocol {
	member, _ := cluster.Member()
	log := logging.GetLogger("atomix", "proxy", string(member.ID))
	partitions := cluster.Partitions()
	proxyPartitions := make([]*Partition, 0, len(partitions))
	proxyPartitionsByID := make(map[PartitionID]*Partition)
	for _, partition := range partitions {
		proxyPartition := NewPartition(partition, log)
		proxyPartitions = append(proxyPartitions, proxyPartition)
		proxyPartitionsByID[proxyPartition.ID] = proxyPartition
	}
	return &Protocol{
		Server:         server.NewServer(cluster),
		primitives:     primitive.NewPrimitiveTypeRegistry(),
		partitions:     proxyPartitions,
		partitionsByID: proxyPartitionsByID,
		Env:            env,
		log:            log,
	}
}

// Protocol is a state machine protocol
type Protocol struct {
	*server.Server
	partitions     []*Partition
	partitionsByID map[PartitionID]*Partition
	Env            env.DriverEnv
	primitives     *primitive.PrimitiveTypeRegistry
	log            logging.Logger
}

// Name returns the protocol name
func (p *Protocol) Name() string {
	member, _ := p.Cluster.Member()
	return string(member.ID)
}

// Primitives returns the protocol primitives
func (p *Protocol) Primitives() *primitive.PrimitiveTypeRegistry {
	return p.primitives
}

// Start starts the node
func (p *Protocol) Start() error {
	p.log.Info("Starting protocol")
	p.Services().RegisterService(func(s *grpc.Server) {
		for _, primitiveType := range p.Primitives().ListPrimitiveTypes() {
			primitiveType.RegisterServer(s)
		}
	})
	p.Services().RegisterService(func(s *grpc.Server) {
		RegisterPrimitiveServer(s, p, p.Env)
	})
	if err := p.Server.Start(); err != nil {
		p.log.Error(err, "Starting protocol")
		return err
	}
	if err := p.connect(); err != nil {
		p.log.Error(err, "Starting protocol")
		return err
	}
	return nil
}

// Configure configures the protocol
func (p *Protocol) Configure(config protocolapi.ProtocolConfig) error {
	if configurable, ok := p.Cluster.(cluster.ConfigurableCluster); ok {
		return configurable.Update(config)
	}
	return nil
}

// Stop stops the node
func (p *Protocol) Stop() error {
	if err := p.Server.Stop(); err != nil {
		return err
	}
	if err := p.close(); err != nil {
		return err
	}
	return nil
}

func (p *Protocol) Partition(partitionID PartitionID) *Partition {
	return p.partitionsByID[partitionID]
}

func (p *Protocol) PartitionBy(partitionKey []byte) *Partition {
	i, err := util.GetPartitionIndex(partitionKey, len(p.partitions))
	if err != nil {
		panic(err)
	}
	return p.partitions[i]
}

func (p *Protocol) Partitions() []*Partition {
	return p.partitions
}

func (p *Protocol) connect() error {
	return async.IterAsync(len(p.partitions), func(i int) error {
		return p.partitions[i].Connect()
	})
}

func (p *Protocol) close() error {
	return async.IterAsync(len(p.partitions), func(i int) error {
		return p.partitions[i].Close()
	})
}
