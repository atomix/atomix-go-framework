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

package _map

import (
	"context"
	mapapi "github.com/atomix/api/go/atomix/primitive/map"
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"github.com/atomix/go-framework/pkg/atomix/protocol/p2p"
	"google.golang.org/grpc"
	"sync"
	"time"
)

func init() {
	registerServerFunc = func(server *grpc.Server, manager *p2p.Manager) {
		RegisterMapProtocolServer(server, newProtocolServer(newManager(manager)))
	}
	newServiceFunc = func(partition *cluster.Partition) p2p.Service {
		return newService(partition)
	}
}

func newProtocolServer(manager *Manager) MapProtocolServer {
	return &ProtocolServer{
		manager: manager,
	}
}

type ProtocolServer struct {
	manager *Manager
}

func (s *ProtocolServer) getService(header RequestHeader) (GossipService, error) {
	partition, err := s.manager.Partition(header.PartitionID)
	if err != nil {
		return nil, err
	}
	service, err := partition.GetService(header.Service)
	if err != nil {
		return nil, err
	}
	return service.(GossipService), nil
}

func (s *ProtocolServer) Bootstrap(request *BootstrapRequest, stream MapProtocol_BootstrapServer) error {
	service, err := s.getService(request.Header)
	if err != nil {
		return errors.Proto(err)
	}

	entryCh := make(chan Entry)
	errCh := make(chan error)
	go func() {
		err := service.Bootstrap(stream.Context(), entryCh)
		if err != nil {
			errCh <- err
			close(errCh)
			close(entryCh)
		}
	}()

	done := false
	for {
		select {
		case entry, ok := <-entryCh:
			if ok {
				err := stream.Send(&BootstrapResponse{
					Update: Update{
						Entry: entry,
					},
				})
				if err != nil {
					return err
				}
			} else {
				if !done {
					done = true
				} else {
					return err
				}
			}
		case e, ok := <-errCh:
			if ok {
				err = e
			} else {
				if !done {
					done = true
				} else {
					return err
				}
			}
		}
	}
}

func (s *ProtocolServer) Advertise(ctx context.Context, request *AdvertiseRequest) (*AdvertiseResponse, error) {
	service, err := s.getService(request.Header)
	if err != nil {
		return nil, errors.Proto(err)
	}

	requests := make(map[string]Digest)
	for key, digest := range request.Digests {
		request, err := service.Advertise(ctx, key, digest)
		if err != nil {
			return nil, errors.Proto(err)
		} else if request {
			requests[key] = digest
		}
	}
	return &AdvertiseResponse{
		Requests: requests,
	}, nil
}

func (s *ProtocolServer) Update(ctx context.Context, request *UpdateRequest) (*UpdateResponse, error) {
	service, err := s.getService(request.Header)
	if err != nil {
		return nil, errors.Proto(err)
	}

	updates := make([]Update, 0, len(request.Updates))
	for _, in := range request.Updates {
		out, err := service.Update(ctx, in)
		if err != nil {
			return nil, errors.Proto(err)
		}
		if out != nil {
			updates = append(updates, *out)
		}
	}
	return &UpdateResponse{
		Updates: updates,
	}, nil
}

type GossipService interface {
	Bootstrap(context.Context, chan<- Entry) error
	Advertise(context.Context, string, Digest) (request bool, err error)
	Update(context.Context, Update) (update *Update, err error)
}

func newService(partition *cluster.Partition) Service {
	service := &mapService{
		partition: partition,
		entries:   make(map[string]Entry),
	}
	service.start()
	return service
}

type mapService struct {
	partition    *cluster.Partition
	entries      map[string]Entry
	entriesMu    sync.RWMutex
	streams      []ServiceEventsStream
	updatesQueue []Update
	updatesMu    sync.Mutex
	updatesCh     chan []Update
}

func (s *mapService) start() {
	go s.sendUpdates()
	go func() {
		ticker := time.NewTicker()
		for range ticker.C {

		}
	}()
}

func (s *mapService) sentUpdates() {
	for updates := range s.updatesCh {
		for _, replica := range s.partition.Replicas() {

		}
	}
}

func (s *mapService) enqueueUpdate(update Update) {

}

func (s *mapService) Size(ctx context.Context) (*mapapi.SizeOutput, error) {
	s.entriesMu.RLock()
	defer s.entriesMu.RUnlock()
	return &mapapi.SizeOutput{
		Size_: uint32(len(s.entries)),
	}, nil
}

func (s *mapService) Exists(ctx context.Context, input *mapapi.ExistsInput) (*mapapi.ExistsOutput, error) {
	s.entriesMu.RLock()
	defer s.entriesMu.RUnlock()
	_, exists := s.entries[input.Key]
	return &mapapi.ExistsOutput{
		ContainsKey: exists,
	}, nil
}

func (s *mapService) Put(ctx context.Context, input *mapapi.PutInput) (*mapapi.PutOutput, error) {
	s.entriesMu.Lock()
	defer s.entriesMu.Unlock()
	s.entries[input.Key] = Entry{
		Key:   input.Key,
		Value: input.Value,
		Digest: Digest{
			Timestamp: input.Meta.Timestamp,
		},
	}
	return &mapapi.PutOutput{
		Entry: &mapapi.Entry{
			Meta: metaapi.ObjectMeta{
				Timestamp: input.Meta.Timestamp,
			},
			Key:   input.Key,
			Value: input.Value,
		},
	}, nil
}

func (s *mapService) Get(ctx context.Context, input *mapapi.GetInput) (*mapapi.GetOutput, error) {
	s.entriesMu.RLock()
	defer s.entriesMu.RUnlock()
	entry, ok := s.entries[input.Key]
	if !ok {
		return nil, errors.NewNotFound("key '%s' not found", input.Key)
	}
	return &mapapi.GetOutput{
		Entry: &mapapi.Entry{
			Meta: metaapi.ObjectMeta{
				Timestamp: entry.Digest.Timestamp,
			},
			Key:   entry.Key,
			Value: entry.Value,
		},
	}, nil
}

func (s *mapService) Remove(ctx context.Context, input *mapapi.RemoveInput) (*mapapi.RemoveOutput, error) {
	s.entriesMu.Lock()
	defer s.entriesMu.Unlock()
	entry, ok := s.entries[input.Key]
	if !ok {
		return nil, errors.NewNotFound("key '%s' not found", input.Key)
	}
	delete(s.entries, input.Key)
	return &mapapi.RemoveOutput{
		Entry: &mapapi.Entry{
			Meta: metaapi.ObjectMeta{
				Timestamp: entry.Digest.Timestamp,
			},
			Key:   entry.Key,
			Value: entry.Value,
		},
	}, nil
}

func (s *mapService) Clear(ctx context.Context) error {
	s.entriesMu.Lock()
	defer s.entriesMu.Unlock()
	for key, entry := range s.entries {
		entry.Digest.Tombstone = true
		s.entries[key] = entry
	}
	return nil
}

func (s *mapService) Events(ctx context.Context, input *mapapi.EventsInput, stream ServiceEventsStream) error {
	s.entriesMu.Lock()
	defer s.entriesMu.Unlock()
	s.streams = append(s.streams, stream)
	return nil
}

func (s *mapService) Entries(ctx context.Context, input *mapapi.EntriesInput, stream ServiceEntriesStream) error {
	s.entriesMu.RLock()
	defer s.entriesMu.RUnlock()
	for _, entry := range s.entries {
		err := stream.Notify(&mapapi.EntriesOutput{
			Entry: mapapi.Entry{
				Meta: metaapi.ObjectMeta{
					Timestamp: entry.Digest.Timestamp,
				},
				Key:   entry.Key,
				Value: entry.Value,
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *mapService) Snapshot(ctx context.Context, writer ServiceSnapshotWriter) error {
	writer.Close()
	return nil
}

func (s *mapService) Restore(ctx context.Context, entry *mapapi.SnapshotEntry) error {
	return nil
}

func (s *mapService) Bootstrap(ctx context.Context, ch chan<- Entry) error {
	s.entriesMu.RLock()
	defer s.entriesMu.RUnlock()
	for _, entry := range s.entries {
		ch <- entry
	}
	return nil
}

func (s *mapService) Advertise(ctx context.Context, key string, digest Digest) (bool, error) {
	s.entriesMu.RLock()
	entry, ok := s.entries[key]
	s.entriesMu.RUnlock()
	if !ok {
		if !digest.Tombstone {
			return true, nil
		}
		return false, nil
	} else {
		localTimestamp := meta.NewTimestamp(entry.Digest.Timestamp)
		updateTimestamp := meta.NewTimestamp(digest.Timestamp)
		if updateTimestamp.After(localTimestamp) {
			if digest.Tombstone {
				_, err := s.Update(ctx, Update{
					Entry: Entry{
						Key:    key,
						Digest: digest,
					},
				})
				return false, err
			}
			return true, nil
		}
		return false, nil
	}
}

func (s *mapService) Update(ctx context.Context, update Update) (*Update, error) {
	remoteEntry := update.Entry
	s.entriesMu.RLock()
	localEntry, ok := s.entries[remoteEntry.Key]
	s.entriesMu.RUnlock()
	if remoteEntry.Digest.Tombstone {
		if ok {
			localTimestamp := meta.NewTimestamp(localEntry.Digest.Timestamp)
			remoteTimestamp := meta.NewTimestamp(remoteEntry.Digest.Timestamp)
			if remoteTimestamp.After(localTimestamp) {
				s.entries[remoteEntry.Key] = remoteEntry
				return nil, nil
			} else {
				return &Update{
					Entry: localEntry,
				}, nil
			}
		} else {
			return nil, nil
		}
	} else {
		if ok {
			localTimestamp := meta.NewTimestamp(localEntry.Digest.Timestamp)
			remoteTimestamp := meta.NewTimestamp(remoteEntry.Digest.Timestamp)
			if remoteTimestamp.After(localTimestamp) {
				s.entriesMu.Lock()
				defer s.entriesMu.Unlock()
				localEntry, ok := s.entries[remoteEntry.Key]
				if ok {
					localTimestamp := meta.NewTimestamp(localEntry.Digest.Timestamp)
					remoteTimestamp := meta.NewTimestamp(remoteEntry.Digest.Timestamp)
					if remoteTimestamp.After(localTimestamp) {
						s.entries[remoteEntry.Key] = remoteEntry
						return nil, nil
					} else if localTimestamp.After(remoteTimestamp) {
						return &Update{
							Entry: localEntry,
						}, nil
					} else {
						return nil, nil
					}
				} else {
					s.entries[remoteEntry.Key] = remoteEntry
					return nil, nil
				}
			} else if localTimestamp.After(remoteTimestamp) {
				return &Update{
					Entry: localEntry,
				}, nil
			} else {
				return nil, nil
			}
		} else {
			s.entriesMu.Lock()
			defer s.entriesMu.Unlock()
			localEntry, ok := s.entries[remoteEntry.Key]
			if ok {
				localTimestamp := meta.NewTimestamp(localEntry.Digest.Timestamp)
				remoteTimestamp := meta.NewTimestamp(remoteEntry.Digest.Timestamp)
				if remoteTimestamp.After(localTimestamp) {
					s.entries[remoteEntry.Key] = remoteEntry
					return nil, nil
				} else if localTimestamp.After(remoteTimestamp) {
					return &Update{
						Entry: localEntry,
					}, nil
				} else {
					return nil, nil
				}
			} else {
				s.entries[remoteEntry.Key] = remoteEntry
				return nil, nil
			}
		}
	}
}
