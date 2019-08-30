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

package registry

// Import all services to ensure they're registered
import (
	_ "github.com/atomix/atomix-go-node/pkg/atomix/counter"
	_ "github.com/atomix/atomix-go-node/pkg/atomix/election"
	_ "github.com/atomix/atomix-go-node/pkg/atomix/list"
	_ "github.com/atomix/atomix-go-node/pkg/atomix/lock"
	_ "github.com/atomix/atomix-go-node/pkg/atomix/map"
	_ "github.com/atomix/atomix-go-node/pkg/atomix/primitive"
	_ "github.com/atomix/atomix-go-node/pkg/atomix/set"
	_ "github.com/atomix/atomix-go-node/pkg/atomix/value"
)

import "github.com/atomix/atomix-go-node/pkg/atomix/service"

var Registry = service.GetRegistry()
