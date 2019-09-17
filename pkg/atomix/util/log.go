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

package util

import (
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

const (
	nodeIDField           = "nodeID"
	serviceNameField      = "serviceName"
	serviceNamespaceField = "serviceNamespace"
	sessionIDField        = "sessionID"
	streamIDField         = "streamID"
)

// NodeEntry returns a log Entry with additional fields containing the given node metadata
func NodeEntry(nodeID string) *log.Entry {
	return log.WithField(nodeIDField, nodeID)
}

// ServiceEntry returns a log Entry with additional fields containing the given service metadata
func ServiceEntry(nodeID string, serviceNamespace string, serviceName string) *log.Entry {
	return log.WithField(nodeIDField, nodeID).
		WithField(serviceNamespaceField, serviceNamespace).
		WithField(serviceNameField, serviceName)
}

// SessionEntry returns a log Entry with additional fields containing the given session metadata
func SessionEntry(nodeID string, serviceNamespace string, serviceName string, sessionID uint64) *log.Entry {
	return log.WithField(nodeIDField, nodeID).
		WithField(serviceNamespaceField, serviceNamespace).
		WithField(serviceNameField, serviceName).
		WithField(sessionIDField, sessionID)
}

// StreamEntry returns a log Entry with additional fields containing the given stream metadata
func StreamEntry(nodeID string, serviceNamespace string, serviceName string, sessionID uint64, streamID uint64) *log.Entry {
	return log.WithField(nodeIDField, nodeID).
		WithField(serviceNamespaceField, serviceNamespace).
		WithField(serviceNameField, serviceName).
		WithField(sessionIDField, sessionID).
		WithField(streamIDField, streamID)
}

type nodeFormatter struct{}

func (f *nodeFormatter) Format(entry *log.Entry) ([]byte, error) {
	buf := bytes.Buffer{}
	buf.Write([]byte(entry.Time.Format(time.StampMilli)))
	buf.Write([]byte(" "))
	buf.Write([]byte(fmt.Sprintf("%-6v", strings.ToUpper(entry.Level.String()))))
	buf.Write([]byte(" "))

	memberID := entry.Data["memberID"]
	if memberID != nil {
		buf.Write([]byte(fmt.Sprintf("%-10v", memberID)))
		buf.Write([]byte(" "))
	}

	serviceNamespace := entry.Data["serviceNamespace"]
	if serviceNamespace != nil {
		buf.Write([]byte(fmt.Sprintf("%-10v", serviceNamespace)))
		buf.Write([]byte(" "))
	}

	serviceName := entry.Data["serviceName"]
	if serviceName != nil {
		buf.Write([]byte(fmt.Sprintf("%-10v", serviceName)))
		buf.Write([]byte(" "))
	}

	sessionID := entry.Data["sessionID"]
	if sessionID != nil {
		buf.Write([]byte(fmt.Sprintf("%-6v", sessionID)))
		buf.Write([]byte(" "))
	}

	streamID := entry.Data["streamID"]
	if streamID != nil {
		buf.Write([]byte(fmt.Sprintf("%-6v", streamID)))
		buf.Write([]byte(" "))
	}

	buf.Write([]byte(entry.Message))
	buf.Write([]byte("\n"))
	return buf.Bytes(), nil
}

func init() {
	log.SetFormatter(&nodeFormatter{})
}
