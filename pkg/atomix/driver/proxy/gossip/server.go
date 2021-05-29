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

package gossip

import (
	"github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
)

func NewServer(client *Client, config GossipConfig) *Server {
	partitions := client.partitions
	proxyPartitions := make([]*Partition, 0, len(partitions))
	proxyPartitionsByID := make(map[PartitionID]*Partition)
	scheme := getTimeScheme(config)
	schemes := make([]time.Scheme, 0, len(partitions))
	for _, partition := range partitions {
		proxyPartition := newPartition(partition, scheme.NewClock(), int(config.ReplicationFactor))
		proxyPartitions = append(proxyPartitions, proxyPartition)
		proxyPartitionsByID[proxyPartition.ID] = proxyPartition
		schemes = append(schemes, scheme)
	}
	return &Server{
		clock:          time.NewCompositeClock(schemes...),
		partitions:     proxyPartitions,
		partitionsByID: proxyPartitionsByID,
	}
}

type Server struct {
	clock          time.Clock
	partitions     []*Partition
	partitionsByID map[PartitionID]*Partition
}

func (s *Server) Partition(partitionID PartitionID) *Partition {
	return s.partitionsByID[partitionID]
}

func (s *Server) PartitionBy(partitionKey []byte) *Partition {
	i, err := util.GetPartitionIndex(partitionKey, len(s.partitions))
	if err != nil {
		panic(err)
	}
	return s.partitions[i]
}

func (s *Server) Partitions() []*Partition {
	return s.partitions
}

func (s *Server) addTimestamp(timestamp *meta.Timestamp) *meta.Timestamp {
	var t time.Timestamp
	if timestamp != nil {
		t = s.clock.Update(time.NewTimestamp(*timestamp))
	} else {
		t = s.clock.Increment()
	}
	proto := s.clock.Scheme().Codec().EncodeTimestamp(t)
	return &proto
}

func (s *Server) AddResponseHeaders(headers *primitive.ResponseHeaders) {
	headers.Timestamp = s.addTimestamp(headers.Timestamp)
}

// getTimeScheme returns the time scheme for the given configuration
func getTimeScheme(config GossipConfig) time.Scheme {
	if config.Clock == nil {
		return time.LogicalScheme
	}
	switch config.Clock.Clock.(type) {
	case *GossipClock_Logical:
		return time.LogicalScheme
	case *GossipClock_Physical:
		return time.PhysicalScheme
	case *GossipClock_Epoch:
		panic("Epoch clock not supported!")
	}
	return time.LogicalScheme
}
