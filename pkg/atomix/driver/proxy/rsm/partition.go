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
	"fmt"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/async"
	"google.golang.org/grpc"
	"sync"
	"time"
)

// NewPartition creates a new proxy partition
func NewPartition(partition cluster.Partition) *Partition {
	return &Partition{
		Partition: partition,
		ID:        PartitionID(partition.ID()),
	}
}

// PartitionID is a partition identifier
type PartitionID int

// Partition is a proxy partition
type Partition struct {
	cluster.Partition
	ID             PartitionID
	sessionTimeout time.Duration
	sessions       map[rsm.ServiceID]*Session
	conn           *grpc.ClientConn
	ticker         *time.Ticker
	mu             sync.RWMutex
}

func (p *Partition) GetSession(ctx context.Context, serviceID rsm.ServiceID) (*Session, error) {
	log.Infof("Opening session %s on partition %d", serviceID, p.ID)
	p.mu.RLock()
	session, ok := p.sessions[serviceID]
	p.mu.RUnlock()
	if ok {
		return session, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	session, ok = p.sessions[serviceID]
	if ok {
		return session, nil
	}

	session = newSession(p.conn, serviceID)
	err := session.open(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("Opening session %s on partition %d", serviceID, p.ID))
		return nil, err
	}
	p.sessions[serviceID] = session
	return session, nil
}

func (p *Partition) open(ctx context.Context) error {
	conn, err := p.Partition.Connect(ctx)
	if err != nil {
		return err
	}
	p.conn = conn

	go func() {
		for range p.ticker.C {
			go p.keepAliveSessions(context.Background())
		}
	}()
	return nil
}

func (p *Partition) keepAliveSessions(ctx context.Context) error {
	p.mu.RLock()
	sessionStates := make(map[rsm.SessionID]*rsm.SessionKeepAlive)
	for _, session := range p.sessions {
		sessionStates[session.sessionID] = session.getState()
	}
	p.mu.RUnlock()

	request := &rsm.PartitionCommandRequest{
		Request: rsm.CommandRequest{
			Request: &rsm.CommandRequest_KeepAlive{
				KeepAlive: &rsm.KeepAliveRequest{
					Sessions: sessionStates,
				},
			},
		},
	}
	client := rsm.NewPartitionServiceClient(p.conn)
	_, err := client.Command(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (p *Partition) close(ctx context.Context) error {
	log.Infof("Closing sessions for partition %d", p.ID)
	p.mu.RLock()
	sessions := make([]*Session, 0, len(p.sessions))
	for _, session := range p.sessions {
		sessions = append(sessions, session)
	}
	p.mu.RUnlock()
	return async.IterAsync(len(sessions), func(i int) error {
		return sessions[i].close(ctx)
	})
}
