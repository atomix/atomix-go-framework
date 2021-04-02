
package value

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip"
	"google.golang.org/grpc"
	value "github.com/atomix/api/go/atomix/primitive/value"
	sync "sync"
	async "github.com/atomix/go-framework/pkg/atomix/util/async"
)

// RegisterServer registers the primitive on the given node
func RegisterServer(node *gossip.Node) {
	node.RegisterServer(func(server *grpc.Server, manager *gossip.Manager) {
		value.RegisterValueServiceServer(server, newServer(manager))
	})
}

func newServer(manager *gossip.Manager) value.ValueServiceServer {
	return &Server{
		manager: manager,
		log: logging.GetLogger("atomix", "protocol", "gossip", "value"),
	}
}
type Server struct {
    manager *gossip.Manager
	log logging.Logger
}

func (s *Server) Set(ctx context.Context, request *value.SetRequest) (*value.SetResponse, error) {
	s.log.Debugf("Received SetRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
    partition, err := s.manager.PartitionFrom(ctx)
    if err != nil {
        s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
        return nil, err
    }

    serviceID := gossip.ServiceId{
        Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
        Namespace: request.Headers.PrimitiveID.Namespace,
        Name:      request.Headers.PrimitiveID.Name,
    }

    service, err := partition.GetService(ctx, serviceID)
    if err != nil {
        s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }

    response, err := service.(Service).Set(ctx, request)
    if err != nil {
        s.log.Errorf("Request SetRequest %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }
    s.manager.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending SetResponse %+v", response)
	return response, nil
}


func (s *Server) Get(ctx context.Context, request *value.GetRequest) (*value.GetResponse, error) {
	s.log.Debugf("Received GetRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
    partition, err := s.manager.PartitionFrom(ctx)
    if err != nil {
        s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
        return nil, err
    }

    serviceID := gossip.ServiceId{
        Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
        Namespace: request.Headers.PrimitiveID.Namespace,
        Name:      request.Headers.PrimitiveID.Name,
    }

    service, err := partition.GetService(ctx, serviceID)
    if err != nil {
        s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }

    response, err := service.(Service).Get(ctx, request)
    if err != nil {
        s.log.Errorf("Request GetRequest %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }
    s.manager.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending GetResponse %+v", response)
	return response, nil
}


func (s *Server) Events(request *value.EventsRequest, srv value.ValueService_EventsServer) error {
    s.log.Debugf("Received EventsRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)

    partitions, err := s.manager.PartitionsFrom(srv.Context())
    if err != nil {
        s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
        return errors.Proto(err)
    }

    serviceID := gossip.ServiceId{
        Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
        Namespace: request.Headers.PrimitiveID.Namespace,
        Name:      request.Headers.PrimitiveID.Name,
    }

    responseCh := make(chan value.EventsResponse)
    wg := &sync.WaitGroup{}
    wg.Add(len(partitions))
    err = async.IterAsync(len(partitions), func(i int) error {
        partition := partitions[i]
        service, err := partition.GetService(srv.Context(), serviceID)
        if err != nil {
            return err
        }

        partitionCh := make(chan value.EventsResponse)
		errCh := make(chan error)
		go func() {
            err := service.(Service).Events(srv.Context(), request, partitionCh)
			if err != nil {
				errCh <- err
			}
			close(errCh)
		}()

		go func() {
			defer wg.Done()
			for response := range partitionCh {
				responseCh <- response
			}
		}()
		return <-errCh
    })
    if err != nil {
        s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
        return errors.Proto(err)
    }

    go func() {
        wg.Wait()
        close(responseCh)
    }()

    for {
        select {
        case response, ok := <-responseCh:
            if ok {
                s.manager.PrepareResponse(&response.Headers)
                s.log.Debugf("Sending EventsResponse %v", response)
                err = srv.Send(&response)
                if err != nil {
                    s.log.Errorf("Request EventsRequest %+v failed: %v", request, err)
                    return errors.Proto(err)
                }
            } else {
                s.log.Debugf("Finished EventsRequest %+v", request)
                return nil
            }
        case <-srv.Context().Done():
            s.log.Debugf("Finished EventsRequest %+v", request)
            return nil
        }
    }
}

