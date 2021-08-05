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
	"math/rand"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

func init() {
	balancer.Register(base.NewBalancerBuilder(resolverName, &PickerBuilder{}, base.Config{}))
}

type PickerBuilder struct{}

func (p *PickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	var leader balancer.SubConn
	var followers []balancer.SubConn
	for sc, scInfo := range info.ReadySCs {
		isLeader := scInfo.Address.Attributes.Value("is_leader").(bool)
		if isLeader {
			leader = sc
			continue
		}
		followers = append(followers, sc)
	}
	if leader == nil && len(followers) > 0 {
		leader = followers[rand.Intn(len(followers))]
	}
	log.Debugf("Built new picker. Leader: %s, Followers: %s", leader, followers)
	return &Picker{
		leader:    leader,
		followers: followers,
	}
}

var _ base.PickerBuilder = (*PickerBuilder)(nil)

type Picker struct {
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var result balancer.PickResult
	if info.FullMethodName == "/atomix.service.PartitionService/Command" ||
		info.FullMethodName == "/atomix.service.PartitionService/CommandStream" ||
		len(p.followers) == 0 {
		result.SubConn = p.leader
	} else if info.FullMethodName == "/atomix.service.PartitionService/Query" ||
		info.FullMethodName == "/atomix.service.PartitionService/QueryStream" {
		result.SubConn = p.nextFollower()
	}
	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}
	return result, nil
}

func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))
	len := uint64(len(p.followers))
	idx := int(cur % len)
	return p.followers[idx]
}

var _ balancer.Picker = (*Picker)(nil)
