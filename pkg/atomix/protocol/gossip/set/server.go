
package set

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	"google.golang.org/grpc"
	set "github.com/atomix/api/go/atomix/primitive/set"
	sync "sync"
	async "github.com/atomix/go-framework/pkg/atomix/util/async"
	
	
)

// RegisterServer registers the primitive on the given node
func RegisterServer(node *gossip.Node) {
	node.RegisterServer(func(server *grpc.Server, manager *gossip.Manager) {
		set.RegisterSetServiceServer(server, newServer(manager))
	})
}

func newServer(manager *gossip.Manager) set.SetServiceServer {
	return &Server{
		manager: manager,
		log: logging.GetLogger("atomix", "protocol", "gossip", "set"),
	}
}
type Server struct {
    manager *gossip.Manager
	log logging.Logger
}

func (s *Server) Size(ctx context.Context, request *set.SizeRequest) (*set.SizeResponse, error) {
	s.log.Debugf("Received SizeRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
	partitions, err := s.manager.PartitionsFrom(ctx)
	if err != nil {
        s.log.Errorf("Request SizeRequest %+v failed: %v", request, err)
	    return nil, errors.Proto(err)
	}

    serviceID := gossip.ServiceId{
        Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
        Namespace: request.Headers.PrimitiveID.Namespace,
        Name:      request.Headers.PrimitiveID.Name,
    }

	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
	    partition := partitions[i]
	    service, err := partition.GetService(ctx, serviceID)
	    if err != nil {
	        return nil, err
	    }
	    response, err := service.(Service).Size(ctx, request)
	    if err != nil {
	        return nil, err
	    }
    	s.manager.PrepareResponse(&response.Headers)
    	return response, nil
    })
	if err != nil {
        s.log.Errorf("Request SizeRequest %+v failed: %v", request, err)
	    return nil, errors.Proto(err)
	}

    response := responses[0].(*set.SizeResponse)
    for _, r := range responses {
        response.Size_ += r.(*set.SizeResponse).Size_
    }
	s.log.Debugf("Sending SizeResponse %+v", response)
	return response, nil
}


func (s *Server) Contains(ctx context.Context, request *set.ContainsRequest) (*set.ContainsResponse, error) {
	s.log.Debugf("Received ContainsRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
    partition, err := s.manager.PartitionFrom(ctx)
    if err != nil {
        s.log.Errorf("Request ContainsRequest %+v failed: %v", request, err)
        return nil, err
    }

    serviceID := gossip.ServiceId{
        Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
        Namespace: request.Headers.PrimitiveID.Namespace,
        Name:      request.Headers.PrimitiveID.Name,
    }

    service, err := partition.GetService(ctx, serviceID)
    if err != nil {
        s.log.Errorf("Request ContainsRequest %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }

    response, err := service.(Service).Contains(ctx, request)
    if err != nil {
        s.log.Errorf("Request ContainsRequest %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }
    s.manager.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending ContainsResponse %+v", response)
	return response, nil
}


func (s *Server) Add(ctx context.Context, request *set.AddRequest) (*set.AddResponse, error) {
	s.log.Debugf("Received AddRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
    partition, err := s.manager.PartitionFrom(ctx)
    if err != nil {
        s.log.Errorf("Request AddRequest %+v failed: %v", request, err)
        return nil, err
    }

    serviceID := gossip.ServiceId{
        Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
        Namespace: request.Headers.PrimitiveID.Namespace,
        Name:      request.Headers.PrimitiveID.Name,
    }

    service, err := partition.GetService(ctx, serviceID)
    if err != nil {
        s.log.Errorf("Request AddRequest %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }

    response, err := service.(Service).Add(ctx, request)
    if err != nil {
        s.log.Errorf("Request AddRequest %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }
    s.manager.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending AddResponse %+v", response)
	return response, nil
}


func (s *Server) Remove(ctx context.Context, request *set.RemoveRequest) (*set.RemoveResponse, error) {
	s.log.Debugf("Received RemoveRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
    partition, err := s.manager.PartitionFrom(ctx)
    if err != nil {
        s.log.Errorf("Request RemoveRequest %+v failed: %v", request, err)
        return nil, err
    }

    serviceID := gossip.ServiceId{
        Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
        Namespace: request.Headers.PrimitiveID.Namespace,
        Name:      request.Headers.PrimitiveID.Name,
    }

    service, err := partition.GetService(ctx, serviceID)
    if err != nil {
        s.log.Errorf("Request RemoveRequest %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }

    response, err := service.(Service).Remove(ctx, request)
    if err != nil {
        s.log.Errorf("Request RemoveRequest %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }
    s.manager.PrepareResponse(&response.Headers)
	s.log.Debugf("Sending RemoveResponse %+v", response)
	return response, nil
}


func (s *Server) Clear(ctx context.Context, request *set.ClearRequest) (*set.ClearResponse, error) {
	s.log.Debugf("Received ClearRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)
	partitions, err := s.manager.PartitionsFrom(ctx)
	if err != nil {
        s.log.Errorf("Request ClearRequest %+v failed: %v", request, err)
	    return nil, errors.Proto(err)
	}

    serviceID := gossip.ServiceId{
        Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
        Namespace: request.Headers.PrimitiveID.Namespace,
        Name:      request.Headers.PrimitiveID.Name,
    }

	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
	    partition := partitions[i]
	    service, err := partition.GetService(ctx, serviceID)
	    if err != nil {
	        return nil, err
	    }
	    response, err := service.(Service).Clear(ctx, request)
	    if err != nil {
	        return nil, err
	    }
    	s.manager.PrepareResponse(&response.Headers)
    	return response, nil
    })
	if err != nil {
        s.log.Errorf("Request ClearRequest %+v failed: %v", request, err)
	    return nil, errors.Proto(err)
	}

    response := responses[0].(*set.ClearResponse)
	s.log.Debugf("Sending ClearResponse %+v", response)
	return response, nil
}


func (s *Server) Events(request *set.EventsRequest, srv set.SetService_EventsServer) error {
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

    responseCh := make(chan set.EventsResponse)
    wg := &sync.WaitGroup{}
    wg.Add(len(partitions))
    err = async.IterAsync(len(partitions), func(i int) error {
        partition := partitions[i]
        service, err := partition.GetService(srv.Context(), serviceID)
        if err != nil {
            return err
        }

        partitionCh := make(chan set.EventsResponse)
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


func (s *Server) Elements(request *set.ElementsRequest, srv set.SetService_ElementsServer) error {
    s.log.Debugf("Received ElementsRequest %+v", request)
	s.manager.PrepareRequest(&request.Headers)

    partitions, err := s.manager.PartitionsFrom(srv.Context())
    if err != nil {
        s.log.Errorf("Request ElementsRequest %+v failed: %v", request, err)
        return errors.Proto(err)
    }

    serviceID := gossip.ServiceId{
        Type:      gossip.ServiceType(request.Headers.PrimitiveID.Type),
        Namespace: request.Headers.PrimitiveID.Namespace,
        Name:      request.Headers.PrimitiveID.Name,
    }

    responseCh := make(chan set.ElementsResponse)
    wg := &sync.WaitGroup{}
    wg.Add(len(partitions))
    err = async.IterAsync(len(partitions), func(i int) error {
        partition := partitions[i]
        service, err := partition.GetService(srv.Context(), serviceID)
        if err != nil {
            return err
        }

        partitionCh := make(chan set.ElementsResponse)
		errCh := make(chan error)
		go func() {
            err := service.(Service).Elements(srv.Context(), request, partitionCh)
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
        s.log.Errorf("Request ElementsRequest %+v failed: %v", request, err)
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
                s.log.Debugf("Sending ElementsResponse %v", response)
                err = srv.Send(&response)
                if err != nil {
                    s.log.Errorf("Request ElementsRequest %+v failed: %v", request, err)
                    return errors.Proto(err)
                }
            } else {
                s.log.Debugf("Finished ElementsRequest %+v", request)
                return nil
            }
        case <-srv.Context().Done():
            s.log.Debugf("Finished ElementsRequest %+v", request)
            return nil
        }
    }
}

