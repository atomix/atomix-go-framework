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
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/golang/protobuf/proto"
	"time"
)

// RegisterService registers the lock service in the given service registry
func RegisterService(registry *service.Registry) {
	registry.Register("lock", newService)
}

// newService returns a new Service
func newService(context service.Context) service.Service {
	service := &Service{
		SessionizedService: service.NewSessionizedService(context),
		queue:              list.New(),
		timers:             make(map[uint64]service.Timer),
	}
	service.init()
	return service
}

// Service is a state machine for a list primitive
type Service struct {
	*service.SessionizedService
	lock   *lockHolder
	queue  *list.List
	timers map[uint64]service.Timer
}

type lockHolder struct {
	index   uint64
	session uint64
	expire  *time.Time
	ch      chan<- service.Result
}

// init initializes the lock service
func (l *Service) init() {
	l.Executor.Register("lock", l.Lock)
	l.Executor.Register("unlock", l.Unlock)
	l.Executor.Register("islocked", l.IsLocked)
}

// Backup backs up the lock service
func (l *Service) Backup() ([]byte, error) {
	var lock *LockCall
	if l.lock != nil {
		lock = &LockCall{
			Index:     int64(l.lock.index),
			SessionId: int64(l.lock.session),
			Expire:    l.lock.expire,
		}
	}

	queue := make([]*LockCall, 0, l.queue.Len())
	element := l.queue.Front()
	for element != nil {
		holder := element.Value.(*lockHolder)
		queue = append(queue, &LockCall{
			Index:     int64(holder.index),
			SessionId: int64(holder.session),
			Expire:    holder.expire,
		})
		element = element.Next()
	}

	snapshot := &LockSnapshot{
		Lock:  lock,
		Queue: queue,
	}
	return proto.Marshal(snapshot)
}

// Restore restores the lock service
func (l *Service) Restore(bytes []byte) error {
	snapshot := &LockSnapshot{}
	if err := proto.Unmarshal(bytes, snapshot); err != nil {
		return err
	}

	if snapshot.Lock != nil {
		l.lock = &lockHolder{
			index:   uint64(snapshot.Lock.Index),
			session: uint64(snapshot.Lock.SessionId),
			expire:  snapshot.Lock.Expire,
		}
	}

	l.queue = list.New()
	for _, lock := range snapshot.Queue {
		element := l.queue.PushBack(&lockHolder{
			index:   uint64(lock.Index),
			session: uint64(lock.SessionId),
			expire:  lock.Expire,
		})

		if lock.Expire != nil {
			index := uint64(lock.Index)
			l.timers[index] = l.Scheduler.ScheduleOnce(lock.Expire.Sub(l.Context.Timestamp()), func() {
				delete(l.timers, index)
				l.queue.Remove(element)
			})
		}
	}
	return nil
}

// Lock attempts to acquire the lock for the current session
func (l *Service) Lock(bytes []byte, ch chan<- service.Result) {
	request := &LockRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- l.NewFailure(err)
		close(ch)
		return
	}

	session := l.Session()

	if l.lock == nil {
		// If the lock is not already owned, immediately grant the lock to the requester.
		// Note that we still have to publish an event to the session. The event is guaranteed to be received
		// by the client-side primitive after the LOCK response.
		l.lock = &lockHolder{
			index:   l.Context.Index(),
			session: session.ID,
			ch:      ch,
		}

		ch <- l.NewResult(proto.Marshal(&LockResponse{
			Index:    int64(l.Context.Index()),
			Acquired: true,
		}))
		close(ch)
	} else if request.Timeout != nil && int64(*request.Timeout) == 0 {
		// If the timeout is 0, that indicates this is a tryLock request. Immediately fail the request.
		ch <- l.NewResult(proto.Marshal(&LockResponse{
			Acquired: false,
		}))
		close(ch)
	} else if request.Timeout != nil {
		// If a timeout exists, add the request to the queue and set a timer. Note that the lock request expiration
		// time is based on the *state machine* time - not the system time - to ensure consistency across servers.
		index := l.Context.Index()
		expire := l.Context.Timestamp().Add(*request.Timeout)
		holder := &lockHolder{
			index:   index,
			session: session.ID,
			expire:  &expire,
			ch:      ch,
		}
		element := l.queue.PushBack(holder)
		l.timers[index] = l.Scheduler.ScheduleOnce(*request.Timeout, func() {
			// When the lock request timer expires, remove the request from the queue and publish a FAILED
			// event to the session. Note that this timer is guaranteed to be executed in the same thread as the
			// state machine commands, so there's no need to use a lock here.
			delete(l.timers, index)
			l.queue.Remove(element)
			ch <- l.NewResult(proto.Marshal(&LockResponse{
				Acquired: false,
			}))
			close(ch)
		})
	} else {
		// If the lock is -1, just add the request to the queue with no expiration.
		holder := &lockHolder{
			index:   l.Context.Index(),
			session: session.ID,
			ch:      ch,
		}
		l.queue.PushBack(holder)
	}
}

// Unlock releases the current lock
func (l *Service) Unlock(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &UnlockRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- l.NewFailure(err)
		return
	}

	session := l.Session()
	if l.lock != nil {
		// If the commit's session does not match the current lock holder, preserve the existing lock.
		// If the current lock ID does not match the requested lock ID, preserve the existing lock.
		// However, ensure the associated lock request is removed from the queue.
		if (request.Index == 0 && l.lock.session != session.ID) || (request.Index > 0 && l.lock.index != uint64(request.Index)) {
			unlocked := false
			element := l.queue.Front()
			for element != nil {
				next := element.Next()
				holder := element.Value.(*lockHolder)
				if (request.Index == 0 && holder.session == session.ID) || (request.Index > 0 && holder.index == uint64(request.Index)) {
					l.queue.Remove(element)
					timer, ok := l.timers[holder.index]
					if ok {
						timer.Cancel()
						delete(l.timers, holder.index)
					}
					unlocked = true
				}
				element = next
			}

			ch <- l.NewResult(proto.Marshal(&UnlockResponse{
				Succeeded: unlocked,
			}))
			return
		}

		// The lock has been released. Populate the lock from the queue.
		element := l.queue.Front()
		if element != nil {
			lock := element.Value.(*lockHolder)
			l.queue.Remove(element)

			// If the waiter has a lock timer, cancel the timer.
			timer, ok := l.timers[lock.index]
			if ok {
				timer.Cancel()
				delete(l.timers, lock.index)
			}

			l.lock = lock

			lock.ch <- l.NewResult(proto.Marshal(&LockResponse{
				Index:    int64(lock.index),
				Acquired: true,
			}))
			close(lock.ch)
		} else {
			l.lock = nil
		}

		ch <- l.NewResult(proto.Marshal(&UnlockResponse{
			Succeeded: true,
		}))
	} else {
		ch <- l.NewResult(proto.Marshal(&UnlockResponse{
			Succeeded: false,
		}))
	}
}

// IsLocked checks whether the lock is held by a specific session
func (l *Service) IsLocked(bytes []byte, ch chan<- service.Result) {
	defer close(ch)

	request := &IsLockedRequest{}
	if err := proto.Unmarshal(bytes, request); err != nil {
		ch <- l.NewFailure(err)
		return
	}

	locked := l.lock != nil && (request.Index == 0 || l.lock.index == uint64(request.Index))
	ch <- l.NewResult(proto.Marshal(&IsLockedResponse{
		Locked: locked,
	}))
}

// OnExpire releases the lock when the owning session expires
func (l *Service) OnExpire(session *service.Session) {
	l.releaseLock(session)
}

// OnClose releases the lock when the owning session is closed
func (l *Service) OnClose(session *service.Session) {
	l.releaseLock(session)
}

func (l *Service) releaseLock(session *service.Session) {
	// Remove all instances of the session from the queue.
	element := l.queue.Front()
	for element != nil {
		next := element.Next()
		lock := element.Value.(*lockHolder)
		if lock.session == session.ID {
			l.queue.Remove(element)
			timer, ok := l.timers[lock.index]
			if ok {
				timer.Cancel()
				delete(l.timers, lock.index)
			}
		}
		element = next
	}

	// If the removed session is the current holder of the lock, nullify the lock and attempt to grant it
	// to the next waiter in the queue.
	if l.lock != nil && l.lock.session == session.ID {
		l.lock = nil

		element := l.queue.Front()
		if element != nil {
			lock := element.Value.(*lockHolder)
			l.queue.Remove(element)

			// If the waiter has a lock timer, cancel the timer.
			timer, ok := l.timers[lock.index]
			if ok {
				timer.Cancel()
				delete(l.timers, lock.index)
			}

			l.lock = lock

			lock.ch <- l.NewResult(proto.Marshal(&LockResponse{
				Index:    int64(lock.index),
				Acquired: true,
			}))
			close(lock.ch)
		}
	}
}
