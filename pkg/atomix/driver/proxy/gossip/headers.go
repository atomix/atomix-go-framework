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

package gossip

import "google.golang.org/grpc/metadata"

const (
	// clusterKeyHeader is the header for setting the cluster key
	clusterKeyHeader = "Cluster-Key"
	// primitiveTypeHeader is the header for setting the primitive type
	primitiveTypeHeader = "Primitive-Type"
	// primitiveNameHeader is the header for setting the primitive name
	primitiveNameHeader = "Primitive-Name"
)

func GetPrimitiveType(md metadata.MD) (string, bool) {
	primitiveTypes := md.Get(primitiveTypeHeader)
	if len(primitiveTypes) == 0 {
		return "", false
	}
	return primitiveTypes[0], true
}

func GetPrimitiveName(md metadata.MD) (string, bool) {
	primitiveNames := md.Get(primitiveNameHeader)
	if len(primitiveNames) == 0 {
		return "", false
	}
	return primitiveNames[0], true
}

func GetClusterKey(md metadata.MD) (string, bool) {
	clusterKeys := md.Get(clusterKeyHeader)
	if len(clusterKeys) == 0 {
		return "", false
	}
	return clusterKeys[0], true
}
