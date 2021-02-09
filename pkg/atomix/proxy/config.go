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

package proxy

import (
	"context"
	"github.com/atomix/api/go/atomix/proxy"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
)

// newConfigServer creates a new proxy config server
func newConfigServer(cluster cluster.Cluster) *configServer {
	return &configServer{
		cluster: cluster,
	}
}

// configServer is a proxy configuration server
type configServer struct {
	cluster cluster.Cluster
}

func (s *configServer) UpdateConfig(ctx context.Context, request *proxy.UpdateConfigRequest) (*proxy.UpdateConfigResponse, error) {
	cluster, ok := s.cluster.(cluster.ConfigurableCluster)
	if !ok {
		return nil, errors.NewNotSupported("protocol does not support configuration changes")
	}
	if err := cluster.Update(request.Config.Protocol); err != nil {
		return nil, err
	}
	return &proxy.UpdateConfigResponse{}, nil
}

var _ proxy.ProxyConfigServiceServer = &configServer{}
