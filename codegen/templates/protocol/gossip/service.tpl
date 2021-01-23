{{- $serviceType := printf "%sServiceType" .Generator.Prefix }}

{{- define "type" }}{{ printf "%s.%s" .Package.Alias .Name }}{{ end }}

package {{ .Package.Name }}

import (
	"context"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/time"
)

var log = logging.GetLogger("atomix", "protocol", "gossip", {{ .Primitive.Name | lower | quote }})

const {{ $serviceType }} gossip.ServiceType = {{ .Primitive.Name | quote }}

// RegisterService registers the service on the given node
func RegisterService(node *gossip.Node) {
	node.RegisterService(ServiceType, func(ctx context.Context, serviceID gossip.ServiceID, partition *gossip.Partition, clock time.Clock) (gossip.Service, error) {
		client, err := newClient(serviceID, partition, clock)
		if err != nil {
			return nil, err
		}
		service := newService(client)
		manager := newManager(client, service)
		return service, manager.start(ctx)
	})
}

var newService func(replicas ReplicationClient) Service

func registerService(f func(replicas ReplicationClient) Service) {
	newService = f
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

type Service interface {
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
