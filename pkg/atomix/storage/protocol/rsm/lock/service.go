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

package lock

import (
	"container/list"
	lockapi "github.com/atomix/atomix-api/go/atomix/primitive/lock"
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/errors"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/storage/protocol/rsm"
)

func init() {
	registerServiceFunc(newService)
}

func newService(context ServiceContext) Service {
	return &lockService{
		ServiceContext: context,
		queue:          list.New(),
		watchers:       make(map[ProposalID]Watcher),
		timers:         make(map[ProposalID]rsm.Timer),
	}
}

// lockService is a state machine for a list primitive
type lockService struct {
	ServiceContext
	owner    *LockRequest
	queue    *list.List
	watchers map[ProposalID]Watcher
	timers   map[ProposalID]rsm.Timer
}

func (l *lockService) Backup(writer SnapshotWriter) error {
	state := &LockState{
		Owner: l.owner,
	}
	request := l.queue.Front()
	for request != nil {
		state.Requests = append(state.Requests, request.Value.(LockRequest))
		request = request.Next()
	}
	return writer.WriteState(state)
}

func (l *lockService) Restore(reader SnapshotReader) error {
	state, err := reader.ReadState()
	if err != nil {
		return err
	}
	l.owner = state.Owner
	for _, request := range state.Requests {
		l.queue.PushBack(request)
	}
	return nil
}

func (l *lockService) Lock(lock LockProposal) {
	lockRequest := LockRequest{
		ProposalID: lock.ID(),
		SessionID:  lock.Session().ID(),
	}
	if lock.Request().Timeout != nil {
		expire := l.Scheduler().Time().Add(*lock.Request().Timeout)
		lockRequest.Expire = &expire
	}

	if l.owner == nil {
		// If the lock is not already owned, immediately grant the lock to the requester.
		// Note that we still have to publish an event to the session. The event is guaranteed to be received
		// by the client-side primitive after the LOCK response.
		l.owner = &lockRequest
		l.watchers[lock.ID()] = lock.Session().Watch(func(state SessionState) {
			if state == SessionClosed {
				l.unlock(lock.Session())
				delete(l.watchers, lock.ID())
			}
		})
		lock.Reply(&lockapi.LockResponse{
			Lock: lockapi.Lock{
				ObjectMeta: metaapi.ObjectMeta{
					Revision: &metaapi.Revision{
						Num: metaapi.RevisionNum(lock.ID()),
					},
				},
				State: lockapi.Lock_LOCKED,
			},
		})
	} else if lock.Request().Timeout != nil && int64(*lock.Request().Timeout) == 0 {
		// If the timeout is 0, that indicates this is a tryLock request. Immediately fail the request.
		lock.Fail(errors.NewTimeout("lock request timed out"))
	} else if lock.Request().Timeout != nil {
		// If a timeout exists, add the request to the queue and set a timer. Note that the lock request expiration
		// time is based on the *state machine* time - not the system time - to ensure consistency across servers.
		element := l.queue.PushBack(lockRequest)
		l.timers[lock.ID()] = l.Scheduler().RunAt(*lockRequest.Expire, func() {
			// When the lock request timer expires, remove the request from the queue and publish a FAILED
			// event to the session. Note that this timer is guaranteed to be executed in the same thread as the
			// state machine commands, so there's no need to use a lock here.
			delete(l.timers, lock.ID())
			l.queue.Remove(element)
			lock.Fail(errors.NewTimeout("lock request timed out"))
		})
		l.watchers[lock.ID()] = lock.Session().Watch(func(state SessionState) {
			if state == SessionClosed {
				l.unlock(lock.Session())
				delete(l.watchers, lock.ID())
			}
		})
	} else {
		// If the lock does not have an expiration, just add the request to the queue with no expiration.
		l.queue.PushBack(lockRequest)
		l.watchers[lock.ID()] = lock.Session().Watch(func(state SessionState) {
			if state == SessionClosed {
				l.unlock(lock.Session())
				delete(l.watchers, lock.ID())
			}
		})
	}
}

func (l *lockService) Unlock(unlock UnlockProposal) (*lockapi.UnlockResponse, error) {
	if l.owner != nil {
		// If the commit's session does not match the current lock holder, preserve the existing lock.
		// If the current lock ID does not match the requested lock ID, preserve the existing lock.
		// However, ensure the associated lock request is removed from the queue.
		if unlock.Session().ID() != l.owner.SessionID {
			return nil, errors.NewConflict("not the lock owner")
		}

		// The lock has been released. Populate the lock from the queue.
		element := l.queue.Front()
		if element != nil {
			request := element.Value.(LockRequest)
			l.queue.Remove(element)

			// If the waiter has a lock timer, cancel the timer.
			timer, ok := l.timers[request.ProposalID]
			if ok {
				timer.Cancel()
				delete(l.timers, request.ProposalID)
			}

			lock, ok := l.Proposals().Lock().Get(request.ProposalID)
			if ok {
				l.owner = &request
				lock.Reply(&lockapi.LockResponse{
					Lock: lockapi.Lock{
						ObjectMeta: metaapi.ObjectMeta{
							Revision: &metaapi.Revision{
								Num: metaapi.RevisionNum(lock.ID()),
							},
						},
						State: lockapi.Lock_LOCKED,
					},
				})
				return &lockapi.UnlockResponse{
					Lock: lockapi.Lock{
						ObjectMeta: metaapi.ObjectMeta{
							Revision: &metaapi.Revision{
								Num: metaapi.RevisionNum(lock.ID()),
							},
						},
						State: lockapi.Lock_LOCKED,
					},
				}, nil
			}
		} else {
			l.owner = nil
			return &lockapi.UnlockResponse{
				Lock: lockapi.Lock{
					State: lockapi.Lock_UNLOCKED,
				},
			}, nil
		}
	}
	return &lockapi.UnlockResponse{
		Lock: lockapi.Lock{
			State: lockapi.Lock_UNLOCKED,
		},
	}, nil
}

func (l *lockService) GetLock(get GetLockQuery) (*lockapi.GetLockResponse, error) {
	if l.owner != nil {
		return &lockapi.GetLockResponse{
			Lock: lockapi.Lock{
				ObjectMeta: metaapi.ObjectMeta{
					Revision: &metaapi.Revision{
						Num: metaapi.RevisionNum(l.owner.ProposalID),
					},
				},
				State: lockapi.Lock_LOCKED,
			},
		}, nil
	}
	return &lockapi.GetLockResponse{
		Lock: lockapi.Lock{
			State: lockapi.Lock_UNLOCKED,
		},
	}, nil
}

func (l *lockService) unlock(session Session) {
	// Remove all instances of the session from the queue.
	element := l.queue.Front()
	for element != nil {
		next := element.Next()
		lock := element.Value.(LockRequest)
		if lock.SessionID == session.ID() {
			l.queue.Remove(element)
			timer, ok := l.timers[lock.ProposalID]
			if ok {
				timer.Cancel()
				delete(l.timers, lock.ProposalID)
			}
		}
		element = next
	}

	// If the removed session is the current holder of the lock, nullify the lock and attempt to grant it
	// to the next waiter in the queue.
	if l.owner != nil && l.owner.SessionID == session.ID() {
		l.owner = nil

		element := l.queue.Front()
		if element != nil {
			lock := element.Value.(LockRequest)
			l.queue.Remove(element)

			// If the waiter has a lock timer, cancel the timer.
			timer, ok := l.timers[lock.ProposalID]
			if ok {
				timer.Cancel()
				delete(l.timers, lock.ProposalID)
			}

			l.owner = &lock

			proposal, ok := l.Proposals().Lock().Get(lock.ProposalID)
			if ok {
				proposal.Reply(&lockapi.LockResponse{
					Lock: lockapi.Lock{
						ObjectMeta: metaapi.ObjectMeta{
							Revision: &metaapi.Revision{
								Num: metaapi.RevisionNum(l.owner.ProposalID),
							},
						},
						State: lockapi.Lock_LOCKED,
					},
				})
			}
		}
	}
}
