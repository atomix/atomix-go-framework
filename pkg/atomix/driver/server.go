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

package coordinator

import (
	"context"
	driverapi "github.com/atomix/api/go/atomix/management/driver"
)

func NewServer(drivers *DriverRegistry, primitives *PrimitiveRegistry) *Server {
	return &Server{
		drivers:    drivers,
		primitives: primitives,
	}
}

type Server struct {
	drivers    *DriverRegistry
	primitives *PrimitiveRegistry
}

func (s *Server) AddProxy(ctx context.Context, request *driverapi.AddProxyRequest) (*driverapi.AddProxyResponse, error) {
	panic("implement me")
}

func (s *Server) RemoveProxy(ctx context.Context, request *driverapi.RemoveProxyRequest) (*driverapi.RemoveProxyResponse, error) {
	panic("implement me")
}

func (s *Server) ConfigureDriver(ctx context.Context, request *driverapi.ConfigureDriverRequest) (*driverapi.ConfigureDriverResponse, error) {
	panic("implement me")
}

var _ driverapi.ProxyManagementServiceServer = &Server{}
var _ driverapi.DriverManagementServiceServer = &Server{}
