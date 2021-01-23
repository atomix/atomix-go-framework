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

const {{ $serviceType }} gossip.ServiceType = {{ .Primitive.Name | quote }}

// Register{{ $serviceInt }} registers the service on the given node
func Register{{ $serviceInt }}(node *gossip.Node) {
	node.RegisterService(ServiceType, func(serviceID gossip.ServiceID, partition *gossip.Partition) (gossip.Replica, error) {
		protocol, err := newProtocol(serviceID, partition)
		if err != nil {
			return nil, err
		}
		service := new{{ $serviceInt }}Func(protocol)
		return &gossipReplica{service: service}, nil
	})
}

var new{{ $serviceInt }}Func func(protocol Protocol) {{ $serviceInt }}

func register{{ $serviceInt }}(f func(protocol Protocol) {{ $serviceInt }}) {
	new{{ $serviceInt }}Func = f
}

type Protocol interface {
    {{- if .Primitive.State.Value }}
	Repair(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) (*{{ template "type" .Primitive.State.Value.Type }}, error)
	Broadcast(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) error
	{{- else if .Primitive.State.Entry }}
	Repair(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) (*{{ template "type" .Primitive.State.Entry.Type }}, error)
	Broadcast(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) error
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

func (p *serviceProtocol) Repair(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) (*{{ template "type" .Primitive.State.Value.Type }}, error) {
	objects, err := p.group.Read(ctx, "")
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		if meta.New(object.ObjectMeta).After(meta.New(value{{ template "field" .Primitive.State.Value.Digest }})) {
			err = proto.Unmarshal(object.Value, value)
			if err != nil {
				return nil, err
			}
		}
	}
	return value, nil
}

func (p *serviceProtocol) Broadcast(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) error {
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

type gossipReplica struct {
	service Service
}

func (s *gossipReplica) Service() gossip.Service {
    return s.service
}

func (s *gossipReplica) Read(ctx context.Context, _ string) (*gossip.Object, error) {
	value, err := s.service.Read(ctx)
	if err != nil {
		return nil, err
	} else if value == nil {
		return nil, nil
	}

	bytes, err := proto.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &gossip.Object{
        {{- if .Primitive.State.Value.Digest }}
        ObjectMeta: value{{ template "field" .Primitive.State.Value.Digest }},
        {{- end }}
		Value:      bytes,
	}, nil
}

func (s *gossipReplica) Update(ctx context.Context, object *gossip.Object) error {
	value := &{{ template "type" .Primitive.State.Value.Type }}{}
	err := proto.Unmarshal(object.Value, value)
	if err != nil {
		return err
	}
	return s.service.Update(ctx, value)
}

func (s *gossipReplica) Clone(ctx context.Context, ch chan<- gossip.Object) error {
    errCh := make(chan error)
    go func() {
        defer close(errCh)
        value, err := s.service.Read(ctx)
        if err != nil {
            errCh <- err
            return
        }
        bytes, err := proto.Marshal(value)
        if err != nil {
            errCh <- err
            return
        }
        object := gossip.Object{
            {{- if .Primitive.State.Value.Digest }}
            ObjectMeta: value{{ template "field" .Primitive.State.Value.Digest }},
            {{- end }}
            Value:      bytes,
        }
        ch <- object
    }()
	return <-errCh
}
{{- else if .Primitive.State.Entry }}
type serviceProtocol struct {
	group *gossip.PeerGroup
}

func (p *serviceProtocol) Repair(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) (*{{ template "type" .Primitive.State.Entry.Type }}, error) {
    {{- if .Primitive.State.Entry.Key }}
	objects, err := p.group.Read(ctx, entry{{ template "field" .Primitive.State.Entry.Key }})
    {{- else }}
	objects, err := p.group.Read(ctx, "")
	{{- end }}
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		if meta.New(object.ObjectMeta).After(meta.New(entry{{ template "field" .Primitive.State.Entry.Digest }})) {
			err = proto.Unmarshal(object.Value, entry)
			if err != nil {
				return nil, err
			}
		}
	}
	return entry, nil
}

func (p *serviceProtocol) Broadcast(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) error {
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

type gossipReplica struct {
	service Service
}

func (s *gossipReplica) Service() gossip.Service {
    return s.service
}

func (s *gossipReplica) Read(ctx context.Context, key string) (*gossip.Object, error) {
	entry, err := s.service.Read(ctx, key)
	if err != nil {
		return nil, err
	} else if entry == nil {
		return nil, nil
	}

	bytes, err := proto.Marshal(entry)
	if err != nil {
		return nil, err
	}
	return &gossip.Object{
        {{- if .Primitive.State.Entry.Digest }}
        ObjectMeta: entry{{ template "field" .Primitive.State.Entry.Digest }},
        {{- end }}
        {{- if .Primitive.State.Entry.Key }}
        Key:        entry{{ template "field" .Primitive.State.Entry.Key }},
        {{- end }}
		Value:      bytes,
	}, nil
}

func (s *gossipReplica) Update(ctx context.Context, object *gossip.Object) error {
	entry := &{{ template "type" .Primitive.State.Entry.Type }}{}
	err := proto.Unmarshal(object.Value, entry)
	if err != nil {
		return err
	}
	return s.service.Update(ctx, entry)
}

func (s *gossipReplica) Clone(ctx context.Context, ch chan<- gossip.Object) error {
	entriesCh := make(chan {{ template "type" .Primitive.State.Entry.Type }})
	errCh := make(chan error)
	go func() {
		err := s.service.List(ctx, entriesCh)
		if err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer close(errCh)
		for entry := range entriesCh {
			bytes, err := proto.Marshal(&entry)
			if err != nil {
				errCh <- err
				return
			}
			object := gossip.Object{
			    {{- if .Primitive.State.Entry.Digest }}
				ObjectMeta: entry{{ template "field" .Primitive.State.Entry.Digest }},
				{{- end }}
				{{- if .Primitive.State.Entry.Key }}
				Key: entry{{ template "field" .Primitive.State.Entry.Key }},
				{{- end }}
				Value:      bytes,
			}
			ch <- object
		}
	}()
	return <-errCh
}
{{- end }}

var _ gossip.Replica = &gossipReplica{}

type Replica interface {
    Protocol() Protocol
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
    Replica
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
