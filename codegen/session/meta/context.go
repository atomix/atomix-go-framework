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

package meta

import (
	"github.com/atomix/atomix-api/go/atomix/primitive/extensions/session"
	"github.com/atomix/atomix-go-sdk/codegen/meta"
	"github.com/lyft/protoc-gen-star"
	"github.com/lyft/protoc-gen-star/lang/go"
)

// NewContext creates a new metadata context
func NewContext(ctx pgsgo.Context) *Context {
	return &Context{
		Context: meta.NewContext(ctx),
		ctx:     ctx,
	}
}

// Context is the code generation context
type Context struct {
	*meta.Context
	ctx pgsgo.Context
}

// GetOptionsFieldMeta extracts the metadata for the options field in the given message
func (c *Context) GetOptionsFieldMeta(message pgs.Message) (*meta.FieldRefMeta, error) {
	return c.FindAnnotatedField(message, session.E_Options)
}

// GetPrimitiveIDFieldMeta extracts the metadata for the primitive ID field in the given message
func (c *Context) GetPrimitiveIDFieldMeta(message pgs.Message) (*meta.FieldRefMeta, error) {
	return c.FindAnnotatedField(message, session.E_PrimitiveId)
}

// GetSessionIDFieldMeta extracts the metadata for the session ID field in the given message
func (c *Context) GetSessionIDFieldMeta(message pgs.Message) (*meta.FieldRefMeta, error) {
	return c.FindAnnotatedField(message, session.E_SessionId)
}
