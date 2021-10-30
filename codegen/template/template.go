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

package template

import (
	"fmt"
	"github.com/atomix/atomix-go-sdk/codegen/meta"
	"github.com/iancoleman/strcase"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"
)

var toCamelCase = func(value string) string {
	return strcase.ToCamel(value)
}

var toLowerCamelCase = func(value string) string {
	return strcase.ToLowerCamel(value)
}

var toLowerCase = func(value string) string {
	return strings.ToLower(value)
}

var toUpperCase = func(value string) string {
	return strings.ToUpper(value)
}

var upperFirst = func(value string) string {
	bytes := []byte(value)
	first := strings.ToUpper(string([]byte{bytes[0]}))
	return string(append([]byte(first), bytes[1:]...))
}

var quote = func(value string) string {
	return "\"" + value + "\""
}

var isLast = func(values interface{}, index int) bool {
	t := reflect.ValueOf(values)
	return index == t.Len()-1
}

var split = func(value, sep string) []string {
	return strings.Split(value, sep)
}

var trim = func(value string) string {
	return strings.Trim(value, " ")
}

var ternary = func(v1, v2 interface{}, b bool) interface{} {
	if b {
		return v1
	}
	return v2
}

// NewTemplate creates a new Template for the given template file
func NewTemplate(file string, imports map[string]meta.PackageMeta) *template.Template {
	paths := make(map[string]string)
	aliases := make(map[string]string)
	for alias, pkg := range imports {
		paths[alias] = pkg.Path
		aliases[pkg.Path] = alias
	}

	t := template.New(path.Base(file))
	funcs := template.FuncMap{
		"toCamel":      toCamelCase,
		"toLowerCamel": toLowerCamelCase,
		"lower":        toLowerCase,
		"upper":        toUpperCase,
		"upperFirst":   upperFirst,
		"quote":        quote,
		"isLast":       isLast,
		"split":        split,
		"trim":         trim,
		"ternary":      ternary,
		"import": func(args ...string) string {
			var path string
			var alias string
			if len(args) == 1 {
				path = args[0]
				alias = filepath.Base(path)
			} else {
				alias = args[0]
				path = args[1]
			}

			if _, ok := aliases[path]; ok {
				return ""
			}

			baseAlias := alias
			i := 0
			for {
				p, ok := paths[alias]
				if ok {
					if p == path {
						return ""
					} else {
						alias = fmt.Sprintf("%s%d", baseAlias, i)
					}
				} else {
					paths[alias] = path
					aliases[path] = alias
					return fmt.Sprintf("%s \"%s\"", alias, path)
				}
				i++
			}
		},
		"alias": func(path string) string {
			return aliases[path]
		},
		"include": func(name string, data interface{}) (string, error) {
			var buf strings.Builder
			err := t.ExecuteTemplate(&buf, name, data)
			return buf.String(), err
		},
	}
	return template.Must(t.Funcs(funcs).ParseFiles(file))
}
