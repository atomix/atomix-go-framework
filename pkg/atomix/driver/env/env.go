// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
