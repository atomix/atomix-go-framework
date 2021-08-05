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

package rsm

import (
	"context"
	"fmt"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const resolverName = "rsm"

func newResolver(partition cluster.Partition) resolver.Builder {
	return &ResolverBuilder{
		partition: partition,
	}
}

type ResolverBuilder struct {
	partition cluster.Partition
}

func (b *ResolverBuilder) Scheme() string {
	return resolverName
}

func (b *ResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}
	dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(retry.RetryingUnaryClientInterceptor(retry.WithRetryOn(codes.Unavailable, codes.Unknown))))
	dialOpts = append(dialOpts, grpc.WithStreamInterceptor(retry.RetryingStreamClientInterceptor(retry.WithRetryOn(codes.Unavailable, codes.Unknown))))
	dialOpts = append(dialOpts, grpc.WithContextDialer(opts.Dialer))

	resolverConn, err := grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}

	serviceConfig := cc.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, resolverName),
	)

	resolver := &Resolver{
		partitionID:   rsm.PartitionID(b.partition.ID()),
		clientConn:    cc,
		resolverConn:  resolverConn,
		serviceConfig: serviceConfig,
	}
	err = resolver.start()
	if err != nil {
		return nil, err
	}
	return resolver, nil
}

var _ resolver.Builder = (*ResolverBuilder)(nil)

type Resolver struct {
	partitionID   rsm.PartitionID
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
}

func (r *Resolver) start() error {
	client := rsm.NewPartitionServiceClient(r.resolverConn)
	request := &rsm.PartitionConfigRequest{
		PartitionID: r.partitionID,
	}
	stream, err := client.WatchConfig(context.Background(), request)
	if err != nil {
		return err
	}
	response, err := stream.Recv()
	if err != nil {
		return err
	}
	r.updateState(response)
	go func() {
		for {
			response, err := stream.Recv()
			if err != nil {
				return
			}
			r.updateState(response)
		}
	}()
	return nil
}

func (r *Resolver) updateState(response *rsm.PartitionConfigResponse) {
	log.Debugf("Updating connections for partition config %+v", response)

	var addrs []resolver.Address
	addrs = append(addrs, resolver.Address{
		Addr: response.Leader,
		Type: resolver.Backend,
		Attributes: attributes.New(
			"is_leader",
			true,
		),
	})

	for _, server := range response.Followers {
		addrs = append(addrs, resolver.Address{
			Addr: server,
			Type: resolver.Backend,
			Attributes: attributes.New(
				"is_leader",
				false,
			),
		})
	}

	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		log.Error("failed to close conn", err)
	}
}

var _ resolver.Resolver = (*Resolver)(nil)
