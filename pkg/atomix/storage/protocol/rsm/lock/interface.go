package lock

import (
	lock "github.com/atomix/api/go/atomix/primitive/lock"
	rsm "github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	proto "github.com/golang/protobuf/proto"
)

type LockResponseFuture struct {
	stream rsm.Stream
	output *lock.LockResponse
	err    error
}

func (f *LockResponseFuture) setStream(stream rsm.Stream) {
	if f.output != nil {
		bytes, err := proto.Marshal(f.output)
		if err != nil {
			stream.Error(err)
		} else {
			stream.Value(bytes)
		}
		stream.Close()
	} else if f.err != nil {
		stream.Error(f.err)
		stream.Close()
	} else {
		f.stream = stream
	}
}

func (f *LockResponseFuture) Complete(output *lock.LockResponse) {
	if f.stream != nil {
		bytes, err := proto.Marshal(output)
		if err != nil {
			f.stream.Error(err)
		} else {
			f.stream.Value(bytes)
		}
		f.stream.Close()
	} else {
		f.output = output
	}
}

func (f *LockResponseFuture) Fail(err error) {
	if f.stream != nil {
		f.stream.Error(err)
		f.stream.Close()
	} else {
		f.err = err
	}
}

type Service interface {
	// Lock attempts to acquire the lock
	Lock(*lock.LockRequest) (*LockResponseFuture, error)
	// Unlock releases the lock
	Unlock(*lock.UnlockRequest) (*lock.UnlockResponse, error)
	// GetLock gets the lock state
	GetLock(*lock.GetLockRequest) (*lock.GetLockResponse, error)
}