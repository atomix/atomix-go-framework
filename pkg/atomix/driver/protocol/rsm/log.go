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
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	logapi "github.com/atomix/api/go/atomix/primitive/log"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	logdriver "github.com/atomix/go-framework/pkg/atomix/driver/primitive/log"
	logro "github.com/atomix/go-framework/pkg/atomix/proxy/ro/log"
	logproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/log"
	"google.golang.org/grpc"
)

func RegisterLogProxy(node *Node) {
	node.RegisterPrimitiveType(newLogType(node))
}

const LogType = "Log"

func newLogType(node *Node) primitive.PrimitiveType {
	return &logType{
		node:     node,
		registry: logdriver.NewLogProxyRegistry(),
	}
}

type logType struct {
	node     *Node
	registry *logdriver.LogProxyRegistry
}

func (p *logType) Name() string {
	return LogType
}

func (p *logType) RegisterServer(s *grpc.Server) {
	logapi.RegisterLogServiceServer(s, logdriver.NewLogProxyServer(p.registry))
}

func (p *logType) AddProxy(config driverapi.ProxyConfig) error {
	server := logproxy.NewLogProxyServer(p.node)
	if !config.Write {
		server = logro.NewReadOnlyLogServer(server)
	}
	return p.registry.AddProxy(getPrimitiveId(config.ID), server)
}

func (p *logType) RemoveProxy(id driverapi.ProxyId) error {
	return p.registry.RemoveProxy(getPrimitiveId(id))
}

var _ primitive.PrimitiveType = &logType{}
