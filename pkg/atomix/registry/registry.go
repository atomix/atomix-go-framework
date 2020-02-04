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
	"github.com/atomix/go-framework/pkg/atomix/node"

	// Register the counter primitive
	_ "github.com/atomix/go-framework/pkg/atomix/counter"

	// Register the election primitive
	_ "github.com/atomix/go-framework/pkg/atomix/election"

	// Register the indexedmap primitive
	_ "github.com/atomix/go-framework/pkg/atomix/indexedmap"

	// Register the log primitive
	_ "github.com/atomix/go-framework/pkg/atomix/log"

	// Register the leader latch primitive
	_ "github.com/atomix/go-framework/pkg/atomix/leader"

	// Register the list primitive
	_ "github.com/atomix/go-framework/pkg/atomix/list"

	// Register the lock primitive
	_ "github.com/atomix/go-framework/pkg/atomix/lock"

	// Register the map primitive
	_ "github.com/atomix/go-framework/pkg/atomix/map"

	// Register the metadata service
	_ "github.com/atomix/go-framework/pkg/atomix/metadata"

	// Register the session management service
	_ "github.com/atomix/go-framework/pkg/atomix/session"

	// Register the set primitive
	_ "github.com/atomix/go-framework/pkg/atomix/set"

	// Register the value primitive
	_ "github.com/atomix/go-framework/pkg/atomix/value"
)

// Registry is a service registry populated with all default primitive services
var Registry = node.GetRegistry()
