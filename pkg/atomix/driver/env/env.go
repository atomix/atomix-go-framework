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

package env

import "os"

const (
	driverNamespaceEnv = "ATOMIX_DRIVER_NAMESPACE"
	driverNameEnv      = "ATOMIX_DRIVER_NAME"
	driverNodeEnv      = "ATOMIX_DRIVER_NODE"
)

// GetDriverEnv gets the driver environment
func GetDriverEnv() DriverEnv {
	return DriverEnv{
		Namespace: os.Getenv(driverNamespaceEnv),
		Name:      os.Getenv(driverNameEnv),
		Node:      os.Getenv(driverNodeEnv),
	}
}

// DriverEnv represents the driver environment
type DriverEnv struct {
	Namespace string
	Name      string
	Node      string
}
