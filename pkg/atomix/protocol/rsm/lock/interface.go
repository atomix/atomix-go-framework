package lock

import (
	lock "github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	"github.com/golang/protobuf/proto"
)

type LockOutputFuture struct {
	stream rsm.Stream
	output *lock.LockOutput
	err    error
}

func (f *LockOutputFuture) setStream(stream rsm.Stream) {
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

func (f *LockOutputFuture) Complete(output *lock.LockOutput) {
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

func (f *LockOutputFuture) Fail(err error) {
	if f.stream != nil {
		f.stream.Error(err)
		f.stream.Close()
	} else {
		f.err = err
	}
}

type Service interface {
	// Lock attempts to acquire the lock
	Lock(*lock.LockInput) (*LockOutputFuture, error)
	// Unlock releases the lock
	Unlock(*lock.UnlockInput) (*lock.UnlockOutput, error)
	// IsLocked checks whether the lock is held
	IsLocked(*lock.IsLockedInput) (*lock.IsLockedOutput, error)
	// Snapshot exports a snapshot of the primitive state
	Snapshot() (*lock.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(*lock.Snapshot) error
}
