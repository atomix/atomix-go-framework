{{- $serviceType := printf "%sServiceType" .Generator.Prefix }}
{{- $serviceInt := printf "%sService" .Generator.Prefix }}
{{- $serviceImpl := printf "%sServiceAdaptor" .Generator.Prefix }}

{{- define "type" }}{{ printf "%s.%s" .Package.Alias .Name }}{{ end }}

{{- define "field" }}
{{- $path := .Field.Path }}
{{- range $index, $element := $path -}}
{{- if eq $index 0 -}}
{{- if isLast $path $index -}}
{{- if $element.Type.IsPointer -}}
.Get{{ $element.Name }}()
{{- else -}}
.{{ $element.Name }}
{{- end -}}
{{- else -}}
{{- if $element.Type.IsPointer -}}
.Get{{ $element.Name }}().
{{- else -}}
.{{ $element.Name }}.
{{- end -}}
{{- end -}}
{{- else -}}
{{- if isLast $path $index -}}
{{- if $element.Type.IsPointer -}}
    Get{{ $element.Name }}()
{{- else -}}
    {{ $element.Name -}}
{{- end -}}
{{- else -}}
{{- if $element.Type.IsPointer -}}
    Get{{ $element.Name }}().
{{- else -}}
    {{ $element.Name }}.
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

package {{ .Package.Name }}

import (
	"context"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
	{{- range .Primitive.Methods }}
	{{- if or .Type.IsAsync .Response.IsStream }}
	{{ import "github.com/golang/protobuf/proto" }}
	{{- end }}
	{{- end }}
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	"github.com/atomix/go-framework/pkg/atomix/meta"
)

type Protocol interface {
    {{- if .Primitive.State.Value }}
	RepairRead(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) (*{{ template "type" .Primitive.State.Value.Type }}, error)
	BroadcastUpdate(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) error
	{{- else if .Primitive.State.Entry }}
	RepairRead(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) (*{{ template "type" .Primitive.State.Entry.Type }}, error)
	BroadcastUpdate(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) error
	{{- end }}
}

func newProtocol(serviceID gossip.ServiceID, partition *gossip.Partition) (Protocol, error) {
	group, err := gossip.NewPeerGroup(partition, ServiceType, serviceID)
	if err != nil {
		return nil, err
	}
	protocol := &serviceProtocol{
		group: group,
	}
	return protocol, nil
}

{{- if .Primitive.State.Value }}
type serviceProtocol struct {
	group *gossip.PeerGroup
}

func (p *serviceProtocol) RepairRead(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) (*{{ template "type" .Primitive.State.Value.Type }}, error) {
	objects, err := p.group.Read(ctx, "")
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		if meta.FromProto(object.ObjectMeta).After(meta.FromProto(value{{ template "field" .Primitive.State.Value.Digest }})) {
			err = proto.Unmarshal(object.Value, value)
			if err != nil {
				return nil, err
			}
		}
	}
	return value, nil
}

func (p *serviceProtocol) BroadcastUpdate(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	object := &gossip.Object{
		ObjectMeta: value{{ template "field" .Primitive.State.Value.Digest }},
		Value:      bytes,
	}
	p.group.Update(ctx, object)
	return nil
}
{{- else if .Primitive.State.Entry }}
type serviceProtocol struct {
	group *gossip.PeerGroup
}

func (p *serviceProtocol) RepairRead(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) (*{{ template "type" .Primitive.State.Entry.Type }}, error) {
    {{- if .Primitive.State.Entry.Key }}
	objects, err := p.group.Read(ctx, entry{{ template "field" .Primitive.State.Entry.Key }})
    {{- else }}
	objects, err := p.group.Read(ctx, "")
	{{- end }}
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		if meta.FromProto(object.ObjectMeta).After(meta.FromProto(entry{{ template "field" .Primitive.State.Entry.Digest }})) {
			err = proto.Unmarshal(object.Value, entry)
			if err != nil {
				return nil, err
			}
		}
	}
	return entry, nil
}

func (p *serviceProtocol) BroadcastUpdate(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) error {
	bytes, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	object := &gossip.Object{
		ObjectMeta: entry{{ template "field" .Primitive.State.Entry.Digest }},
		Value:      bytes,
	}
	p.group.Update(ctx, object)
	return nil
}
{{- end }}

var _ Protocol = &serviceProtocol{}
