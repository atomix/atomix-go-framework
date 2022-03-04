// Code generated by atomix-go-framework. DO NOT EDIT.
package lock

import (
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/gogo/protobuf/proto"
	"io"
)

var log = logging.GetLogger("atomix", "lock", "service")

const Type = "Lock"

const (
	lockOp    = "Lock"
	unlockOp  = "Unlock"
	getLockOp = "GetLock"
)

var newServiceFunc rsm.NewServiceFunc

func registerServiceFunc(rsmf NewServiceFunc) {
	newServiceFunc = func(context rsm.ServiceContext) rsm.Service {
		return &ServiceAdaptor{
			ServiceContext: context,
			rsm:            rsmf(newServiceContext(context)),
		}
	}
}

type NewServiceFunc func(ServiceContext) Service

// RegisterService registers the election primitive service on the given node
func RegisterService(node *rsm.Node) {
	node.RegisterService(Type, newServiceFunc)
}

type ServiceAdaptor struct {
	rsm.ServiceContext
	rsm Service
}

func (s *ServiceAdaptor) ExecuteCommand(command rsm.Command) {
	switch command.OperationID() {
	case 1:
		p, err := newLockProposal(command)
		if err != nil {
			err = errors.NewInternal(err.Error())
			log.Error(err)
			command.Output(nil, err)
			return
		}

		log.Debugf("Proposal LockProposal %s", p)
		s.rsm.Lock(p)
	case 2:
		p, err := newUnlockProposal(command)
		if err != nil {
			err = errors.NewInternal(err.Error())
			log.Error(err)
			command.Output(nil, err)
			return
		}

		log.Debugf("Proposal UnlockProposal %s", p)
		response, err := s.rsm.Unlock(p)
		if err != nil {
			log.Debugf("Proposal UnlockProposal %s failed: %v", p, err)
			command.Output(nil, err)
		} else {
			output, err := proto.Marshal(response)
			if err != nil {
				err = errors.NewInternal(err.Error())
				log.Errorf("Proposal UnlockProposal %s failed: %v", p, err)
				command.Output(nil, err)
			} else {
				log.Debugf("Proposal UnlockProposal %s complete: %+v", p, response)
				command.Output(output, nil)
			}
		}
		command.Close()
	default:
		err := errors.NewNotSupported("unknown operation %d", command.OperationID())
		log.Debug(err)
		command.Output(nil, err)
	}
}

func (s *ServiceAdaptor) ExecuteQuery(query rsm.Query) {
	switch query.OperationID() {
	case 3:
		q, err := newGetLockQuery(query)
		if err != nil {
			err = errors.NewInternal(err.Error())
			log.Error(err)
			query.Output(nil, err)
			return
		}

		log.Debugf("Querying GetLockQuery %s", q)
		response, err := s.rsm.GetLock(q)
		if err != nil {
			log.Debugf("Querying GetLockQuery %s failed: %v", q, err)
			query.Output(nil, err)
		} else {
			output, err := proto.Marshal(response)
			if err != nil {
				err = errors.NewInternal(err.Error())
				log.Errorf("Querying GetLockQuery %s failed: %v", q, err)
				query.Output(nil, err)
			} else {
				log.Debugf("Querying GetLockQuery %s complete: %+v", q, response)
				query.Output(output, nil)
			}
		}
		query.Close()
	default:
		err := errors.NewNotSupported("unknown operation %d", query.OperationID())
		log.Debug(err)
		query.Output(nil, err)
	}
}
func (s *ServiceAdaptor) Backup(writer io.Writer) error {
	err := s.rsm.Backup(newSnapshotWriter(writer))
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (s *ServiceAdaptor) Restore(reader io.Reader) error {
	err := s.rsm.Restore(newSnapshotReader(reader))
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

var _ rsm.Service = &ServiceAdaptor{}
