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
	node.RegisterService(ServiceType, func(serviceID gossip.ServiceID, partition *gossip.Partition) (gossip.Replica, error) {
		protocol, err := newProtocol(serviceID, partition)
		if err != nil {
			return nil, err
		}
		return &serviceReplica{replica: newReplica(protocol)}, nil
	})
}

type {{ $serviceInt }} interface {
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
