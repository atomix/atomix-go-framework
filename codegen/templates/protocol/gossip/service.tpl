{{- $serviceType := printf "%sServiceType" .Generator.Prefix }}
{{- $serviceInt := printf "%sService" .Generator.Prefix }}
{{- $serviceImpl := printf "%sServiceAdaptor" .Generator.Prefix }}

{{- define "type" }}{{ printf "%s.%s" .Package.Alias .Name }}{{ end }}

package {{ .Package.Name }}

import (
	"context"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
)

const {{ $serviceType }} gossip.ServiceType = {{ .Primitive.Name | quote }}

// Register{{ $serviceInt }} registers the service on the given node
func Register{{ $serviceInt }}(node *gossip.Node) {
	node.RegisterService(ServiceType, func(serviceID gossip.ServiceID, partition *gossip.Partition) (gossip.Service, error) {
		client, err := newClient(serviceID, partition)
		if err != nil {
			return nil, err
		}
		return new{{ $serviceInt }}(client), nil
	})
}

var new{{ $serviceInt }} func(replicas ReplicationClient) Service

func register{{ $serviceInt }}(f func(replicas ReplicationClient) Service) {
	new{{ $serviceInt }} = f
}

type Delegate interface {
    {{- if .Primitive.State.Value }}
	Read(ctx context.Context) (*{{ template "type" .Primitive.State.Value.Type }}, error)
	Update(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) error
    {{- else if .Primitive.State.Entry }}
    {{- if .Primitive.State.Entry.Key }}
    {{- if .Primitive.State.Entry.Key.Field.Type.IsScalar }}
	Read(ctx context.Context, key {{ .Primitive.State.Entry.Key.Field.Type.Name }}) (*{{ template "type" .Primitive.State.Entry.Type }}, error)
    {{- else }}
	Read(ctx context.Context, key {{ template "type" .Primitive.State.Entry.Key.Field.Type }}) (*{{ template "type" .Primitive.State.Entry.Type }}, error)
	{{- end }}
    {{- else }}
	Read(ctx context.Context) (*{{ template "type" .Primitive.State.Entry.Type }}, error)
    {{- end }}
    List(ctx context.Context, ch chan<- {{ template "type" .Primitive.State.Entry.Type }}) error
	Update(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) error
    {{- end }}
}

type {{ $serviceInt }} interface {
	gossip.Service
	Delegate() Delegate
    {{- range .Primitive.Methods }}

    {{- $comments := split .Comment "\n" }}
    {{- range $comment := $comments }}
    {{- if $comment }}
    // {{ $comment | trim }}
    {{- end }}
    {{- end }}

    {{- if or .Type.IsAsync .Response.IsStream }}
    {{ .Name }}(context.Context, *{{ template "type" .Request.Type }}, chan<- {{ template "type" .Response.Type }}) error
    {{- else }}
    {{ .Name }}(context.Context, *{{ template "type" .Request.Type }}) (*{{ template "type" .Response.Type }}, error)
    {{- end }}
    {{- end }}
}
