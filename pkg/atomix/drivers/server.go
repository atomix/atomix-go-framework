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

package drivers

import (
	"context"
	driverapi "github.com/atomix/api/go/atomix/driver"
	"github.com/atomix/go-framework/pkg/atomix/logging"
)

var log = logging.GetLogger("atomix", "drivers")

// NewServer returns a new drivers server
func NewServer(registry *Registry) *Server {
	return &Server{
		drivers: registry,
	}
}

type Server struct {
	drivers *Registry
}

func (s *Server) GetDriver(ctx context.Context, request *driverapi.GetDriverRequest) (*driverapi.GetDriverResponse, error) {
	log.Debugf("Received GetDriverRequest %+v", request)
	driver, err := s.drivers.GetDriver(request.Name)
	if err != nil {
		log.Warnf("GetDriverRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &driverapi.GetDriverResponse{
		Driver: driver,
	}
	log.Debugf("Sending GetDriverResponse %+v", response)
	return response, nil
}

func (s *Server) ListDrivers(ctx context.Context, request *driverapi.ListDriversRequest) (*driverapi.ListDriversResponse, error) {
	log.Debugf("Received ListDriversRequest %+v", request)
	drivers, err := s.drivers.ListDrivers()
	if err != nil {
		log.Warnf("ListDriversRequest %+v failed: %v", request, err)
		return nil, err
	}
	response := &driverapi.ListDriversResponse{
		Drivers: drivers,
	}
	log.Debugf("Sending ListDriversResponse %+v", response)
	return response, nil
}

var _ driverapi.DriverServiceServer = &Server{}
