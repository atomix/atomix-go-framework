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
	"github.com/atomix/atomix-go-sdk/codegen/primitive"
	"github.com/atomix/atomix-go-sdk/codegen/session"
	"github.com/lyft/protoc-gen-star"
	pgsgo "github.com/lyft/protoc-gen-star/lang/go"
)

func main() {
	pgs.Init(pgs.DebugMode()).
		RegisterModule(primitive.NewModule("primitive", map[string]string{
			"primitive.go": "/etc/atomix/templates/driver/primitive/primitive.tpl",
			"registry.go":  "/etc/atomix/templates/driver/primitive/registry.tpl",
		})).
		RegisterModule(session.NewModule("session", map[string]string{
			"session.go": "/etc/atomix/templates/driver/session/session.tpl",
			"manager.go": "/etc/atomix/templates/driver/session/manager.tpl",
		})).
		RegisterPostProcessor(pgsgo.GoFmt()).
		Render()
}
