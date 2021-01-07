package lock

import (
	lock "github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"io"
)

const Type = "Lock"

const (
	lockOp     = "Lock"
	unlockOp   = "Unlock"
	isLockedOp = "IsLocked"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
)

var newServiceFunc rsm.NewServiceFunc

func registerServiceFunc(rsmf NewServiceFunc) {
	newServiceFunc = func(scheduler rsm.Scheduler, context rsm.ServiceContext) rsm.Service {
		service := &ServiceAdaptor{
			rsm: rsmf(scheduler, context),
			log: logging.GetLogger("atomix", "lock", "service"),
		}
		service.init()
		return service
	}
}

type NewServiceFunc func(scheduler rsm.Scheduler, context rsm.ServiceContext) Service

// RegisterService registers the election primitive service on the given node
func RegisterService(node *rsm.Node) {
	node.RegisterService(Type, newServiceFunc)
}

type ServiceAdaptor struct {
	rsm.Service
	rsm Service
	log logging.Logger
}

func (s *ServiceAdaptor) init() {
	s.RegisterStreamOperation(lockOp, s.lock)
	s.RegisterUnaryOperation(unlockOp, s.unlock)
	s.RegisterUnaryOperation(isLockedOp, s.isLocked)
	s.RegisterUnaryOperation(snapshotOp, s.snapshot)
	s.RegisterUnaryOperation(restoreOp, s.restore)
}

func (s *ServiceAdaptor) SessionOpen(session rsm.Session) {
	if sessionOpen, ok := s.rsm.(rsm.SessionOpenService); ok {
		sessionOpen.SessionOpen(session)
	}
}

func (s *ServiceAdaptor) SessionExpired(session rsm.Session) {
	if sessionExpired, ok := s.rsm.(rsm.SessionExpiredService); ok {
		sessionExpired.SessionExpired(session)
	}
}

func (s *ServiceAdaptor) SessionClosed(session rsm.Session) {
	if sessionClosed, ok := s.rsm.(rsm.SessionClosedService); ok {
		sessionClosed.SessionClosed(session)
	}
}

func (s *ServiceAdaptor) Backup(writer io.Writer) error {
	snapshot, err := s.rsm.Snapshot()
	if err != nil {
		s.log.Error(err)
		return err
	}
	bytes, err := proto.Marshal(snapshot)
	if err != nil {
		s.log.Error(err)
		return err
	}
	return util.WriteBytes(writer, bytes)
}

func (s *ServiceAdaptor) Restore(reader io.Reader) error {
	bytes, err := util.ReadBytes(reader)
	if err != nil {
		s.log.Error(err)
		return err
	}
	snapshot := &lock.Snapshot{}
	err = proto.Unmarshal(bytes, snapshot)
	if err != nil {
		return err
	}
	err = s.rsm.Restore(snapshot)
	if err != nil {
		s.log.Error(err)
		return err
	}
	return nil
}

func (s *ServiceAdaptor) lock(in []byte, stream rsm.Stream) {
	input := &lock.LockInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		stream.Error(err)
		stream.Close()
		return
	}
	future, err := s.rsm.Lock(input)
	if err != nil {
		s.log.Error(err)
		stream.Error(err)
		stream.Close()
		return
	}
	future.setStream(stream)
}

func (s *ServiceAdaptor) unlock(in []byte) ([]byte, error) {
	input := &lock.UnlockInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.Unlock(input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return out, nil
}

func (s *ServiceAdaptor) isLocked(in []byte) ([]byte, error) {
	input := &lock.IsLockedInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.IsLocked(input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return out, nil
}

func (s *ServiceAdaptor) snapshot(in []byte) ([]byte, error) {
	output, err := s.rsm.Snapshot()
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return out, nil
}

func (s *ServiceAdaptor) restore(in []byte) ([]byte, error) {
	input := &lock.Snapshot{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	err = s.rsm.Restore(input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return nil, nil
}

var _ rsm.Service = &ServiceAdaptor{}
