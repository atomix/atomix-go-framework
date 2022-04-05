// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/atomix/atomix-go-framework/codegen"
	"github.com/lyft/protoc-gen-star"
	pgsgo "github.com/lyft/protoc-gen-star/lang/go"
)

func main() {
	pgs.Init(pgs.DebugMode()).
		RegisterModule(codegen.NewModule("driver", "primitive", map[string]string{
			"server.go":   "/etc/atomix/templates/driver/primitive/server.tpl",
			"registry.go": "/etc/atomix/templates/driver/primitive/registry.tpl",
		})).
		RegisterModule(codegen.NewModule("proxy", "rsm", map[string]string{
			"proxy.go": "/etc/atomix/templates/driver/proxy/rsm/proxy.tpl",
		})).
		RegisterModule(codegen.NewModule("storage", "rsm", map[string]string{
			"interface.go": "/etc/atomix/templates/storage/protocol/rsm/interface.tpl",
			"adapter.go":   "/etc/atomix/templates/storage/protocol/rsm/adapter.tpl",
		})).
		RegisterModule(codegen.NewModule("proxy", "gossip", map[string]string{
			"proxy.go": "/etc/atomix/templates/driver/proxy/gossip/proxy.tpl",
		})).
		RegisterModule(codegen.NewModule("storage", "gossip", map[string]string{
			"gossip.go":  "/etc/atomix/templates/storage/protocol/gossip/gossip.tpl",
			"server.go":  "/etc/atomix/templates/storage/protocol/gossip/server.tpl",
			"service.go": "/etc/atomix/templates/storage/protocol/gossip/service.tpl",
		})).
		RegisterPostProcessor(pgsgo.GoFmt()).
		Render()
}
