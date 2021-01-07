package set

import (
	set "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-framework/pkg/atomix/storage/rsm"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"io"
)

const Type = "Set"

const (
	sizeOp     = "Size"
	containsOp = "Contains"
	addOp      = "Add"
	removeOp   = "Remove"
	clearOp    = "Clear"
	eventsOp   = "Events"
	elementsOp = "Elements"
	snapshotOp = "Snapshot"
	restoreOp  = "Restore"
)

var newServiceFunc rsm.NewServiceFunc

func registerServiceFunc(rsmf NewServiceFunc) {
	newServiceFunc = func(scheduler rsm.Scheduler, context rsm.ServiceContext) rsm.Service {
		service := &ServiceAdaptor{
			rsm: rsmf(scheduler, context),
			log: logging.GetLogger("atomix", "set", "service"),
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
	s.RegisterUnaryOperation(sizeOp, s.size)
	s.RegisterUnaryOperation(containsOp, s.contains)
	s.RegisterUnaryOperation(addOp, s.add)
	s.RegisterUnaryOperation(removeOp, s.remove)
	s.RegisterUnaryOperation(clearOp, s.clear)
	s.RegisterStreamOperation(eventsOp, s.events)
	s.RegisterStreamOperation(elementsOp, s.elements)
	s.RegisterStreamOperation(snapshotOp, s.snapshot)
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
	err := s.rsm.Snapshot(newServiceSnapshotWriter(writer))
	if err != nil {
		s.log.Error(err)
		return err
	}
	return nil
}

func (s *ServiceAdaptor) Restore(reader io.Reader) error {
	for {
		bytes, err := util.ReadBytes(reader)
		if err == io.EOF {
			return nil
		} else if err != nil {
			s.log.Error(err)
			return err
		}

		entry := &set.SnapshotEntry{}
		err = proto.Unmarshal(bytes, entry)
		if err != nil {
			s.log.Error(err)
			return err
		}
		err = s.rsm.Restore(entry)
		if err != nil {
			s.log.Error(err)
			return err
		}
	}
}

func (s *ServiceAdaptor) size(in []byte) ([]byte, error) {
	output, err := s.rsm.Size()
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

func (s *ServiceAdaptor) contains(in []byte) ([]byte, error) {
	input := &set.ContainsInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.Contains(input)
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

func (s *ServiceAdaptor) add(in []byte) ([]byte, error) {
	input := &set.AddInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.Add(input)
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

func (s *ServiceAdaptor) remove(in []byte) ([]byte, error) {
	input := &set.RemoveInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.Remove(input)
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

func (s *ServiceAdaptor) clear(in []byte) ([]byte, error) {
	err := s.rsm.Clear()
	if err != nil {
		s.log.Error(err)
		return nil, err
	}
	return nil, nil
}

func (s *ServiceAdaptor) events(in []byte, stream rsm.Stream) {
	input := &set.EventsInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		stream.Error(err)
		stream.Close()
		return
	}
	output := newServiceEventsStream(stream)
	err = s.rsm.Events(input, output)
	if err != nil {
		s.log.Error(err)
		stream.Error(err)
		stream.Close()
		return
	}
}

func (s *ServiceAdaptor) elements(in []byte, stream rsm.Stream) {
	input := &set.ElementsInput{}
	err := proto.Unmarshal(in, input)
	if err != nil {
		s.log.Error(err)
		stream.Error(err)
		stream.Close()
		return
	}
	output := newServiceElementsStream(stream)
	err = s.rsm.Elements(input, output)
	if err != nil {
		s.log.Error(err)
		stream.Error(err)
		stream.Close()
		return
	}
}

func (s *ServiceAdaptor) snapshot(in []byte, stream rsm.Stream) {
	err := s.rsm.Snapshot(newServiceSnapshotStreamWriter(stream))
	if err != nil {
		s.log.Error(err)
		stream.Error(err)
		stream.Close()
		return
	}
}

func (s *ServiceAdaptor) restore(in []byte) ([]byte, error) {
	input := &set.SnapshotEntry{}
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
