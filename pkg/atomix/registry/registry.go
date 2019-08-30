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
	// Register the counter primitive
	_ "github.com/atomix/atomix-go-node/pkg/atomix/counter"

	// Register the election primitive
	_ "github.com/atomix/atomix-go-node/pkg/atomix/election"

	// Register the list primitive
	_ "github.com/atomix/atomix-go-node/pkg/atomix/list"

	// Register the lock primitive
	_ "github.com/atomix/atomix-go-node/pkg/atomix/lock"

	// Register the map primitive
	_ "github.com/atomix/atomix-go-node/pkg/atomix/map"

	// Register the primitive metadata service
	_ "github.com/atomix/atomix-go-node/pkg/atomix/primitive"

	// Register the set primitive
	_ "github.com/atomix/atomix-go-node/pkg/atomix/set"

	// Register the value primitive
	_ "github.com/atomix/atomix-go-node/pkg/atomix/value"
)

import "github.com/atomix/atomix-go-node/pkg/atomix/service"

// Registry is a service registry populated with all default primitive services
var Registry = service.GetRegistry()
