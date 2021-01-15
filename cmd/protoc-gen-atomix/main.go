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

package main

import (
	"github.com/atomix/go-framework/codegen"
	"github.com/lyft/protoc-gen-star"
	"github.com/lyft/protoc-gen-star/lang/go"
)

func main() {
	pgs.Init(pgs.DebugMode()).
		RegisterModule(codegen.NewModule("proxy", "rsm", map[string]string{
			"proxy.go": "/etc/atomix/templates/proxy/rsm/proxy.tpl",
		})).
		RegisterModule(codegen.NewModule("protocol", "rsm", map[string]string{
			"interface.go": "/etc/atomix/templates/protocol/rsm/interface.tpl",
			"adapter.go":   "/etc/atomix/templates/protocol/rsm/adapter.tpl",
		})).
		RegisterModule(codegen.NewModule("proxy", "gossip", map[string]string{
			"proxy.go": "/etc/atomix/templates/proxy/gossip/proxy.tpl",
		})).
		RegisterModule(codegen.NewModule("protocol", "gossip", map[string]string{
			"manager.go":   "/etc/atomix/templates/protocol/gossip/manager.tpl",
			"partition.go": "/etc/atomix/templates/protocol/gossip/partition.tpl",
			"service.go":   "/etc/atomix/templates/protocol/gossip/service.tpl",
			"server.go":    "/etc/atomix/templates/protocol/gossip/server.tpl",
		})).
		RegisterModule(codegen.NewModule("proxy", "crdt", map[string]string{
			"proxy.go": "/etc/atomix/templates/proxy/crdt/proxy.tpl",
		})).
		RegisterModule(codegen.NewModule("protocol", "crdt", map[string]string{
			"manager.go":   "/etc/atomix/templates/protocol/crdt/manager.tpl",
			"partition.go": "/etc/atomix/templates/protocol/crdt/partition.tpl",
			"service.go":   "/etc/atomix/templates/protocol/crdt/service.tpl",
			"server.go":    "/etc/atomix/templates/protocol/crdt/server.tpl",
		})).
		RegisterModule(codegen.NewModule("client", "", map[string]string{
			"client.go": "/etc/atomix/templates/client/client.tpl",
		})).
		RegisterPostProcessor(pgsgo.GoFmt()).
		Render()
}
