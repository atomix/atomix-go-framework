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
	"math/rand"
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
	"github.com/atomix/go-framework/pkg/atomix/time"
)

func newClient(serviceID gossip.ServiceID, partition *gossip.Partition, clock time.Clock) (ReplicationClient, error) {
	group, err := gossip.NewPeerGroup(partition, ServiceType, serviceID)
	if err != nil {
		return nil, err
	}
	return &replicationClient{
		group: group,
		clock: clock,
	}, nil
}

type ReplicationClient interface {
    Clock() time.Clock
    {{- if .Primitive.State.Value }}
	Bootstrap(ctx context.Context) (*{{ template "type" .Primitive.State.Value.Type }}, error)
	Repair(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) (*{{ template "type" .Primitive.State.Value.Type }}, error)
	Advertise(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) error
	Update(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) error
	{{- else if .Primitive.State.Entry }}
	Bootstrap(ctx context.Context, ch chan<- {{ template "type" .Primitive.State.Entry.Type }}) error
	Repair(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) (*{{ template "type" .Primitive.State.Entry.Type }}, error)
	Advertise(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) error
	Update(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) error
	{{- end }}
}

type replicationClient struct {
	group *gossip.PeerGroup
	clock time.Clock
}

func (p *replicationClient) Clock() time.Clock {
    return p.clock
}

{{- if .Primitive.State.Value }}
func (p *replicationClient) Bootstrap(ctx context.Context) (*{{ template "type" .Primitive.State.Value.Type }}, error) {
	objects, err := p.group.Read(ctx, "")
	if err != nil {
		return nil, err
	}

    var value *{{ template "type" .Primitive.State.Value.Type }}
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

func (p *replicationClient) Repair(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) (*{{ template "type" .Primitive.State.Value.Type }}, error) {
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

func (p *replicationClient) Advertise(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) error {
	peers := p.group.Peers()
	peer := peers[rand.Intn(len(peers))]
	peer.Advertise(ctx, "", meta.FromProto(value{{ template "field" .Primitive.State.Value.Digest }}))
	return nil
}

func (p *replicationClient) Update(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) error {
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
func (p *replicationClient) Bootstrap(ctx context.Context, ch chan<- {{ template "type" .Primitive.State.Entry.Type }}) error {
	objectCh := make(chan gossip.Object)
	if err := p.group.ReadAll(ctx, objectCh); err != nil {
		return err
	}
	go func() {
		for object := range objectCh {
			var entry {{ template "type" .Primitive.State.Entry.Type }}
			err := proto.Unmarshal(object.Value, &entry)
			if err != nil {
				log.Errorf("Bootstrap failed: %v", err)
			} else {
				ch <- entry
			}
		}
	}()
	return nil
}

func (p *replicationClient) Repair(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) (*{{ template "type" .Primitive.State.Entry.Type }}, error) {
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

func (p *replicationClient) Advertise(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) error {
	peers := p.group.Peers()
	peer := peers[rand.Intn(len(peers))]
	peer.Advertise(ctx, entry{{ template "field" .Primitive.State.Entry.Key }}, meta.FromProto(entry{{ template "field" .Primitive.State.Entry.Digest }}))
	return nil
}

func (p *replicationClient) Update(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) error {
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

var _ ReplicationClient = &replicationClient{}

func newReplica(service Service) ReplicationServer {
	return &replicationServer{
		delegate: service.Delegate(),
	}
}

type ReplicationServer interface {
	gossip.Replica
}

{{ if .Primitive.State.Value }}
type replicationServer struct {
	delegate Delegate
}

func (s *replicationServer) Read(ctx context.Context, _ string) (*gossip.Object, error) {
	value, err := s.delegate.Read(ctx)
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

func (s *replicationServer) Update(ctx context.Context, object *gossip.Object) error {
	value := &{{ template "type" .Primitive.State.Value.Type }}{}
	err := proto.Unmarshal(object.Value, value)
	if err != nil {
		return err
	}
	return s.delegate.Update(ctx, value)
}

func (s *replicationServer) ReadAll(ctx context.Context, ch chan<- gossip.Object) error {
    errCh := make(chan error)
    go func() {
        defer close(errCh)
        value, err := s.delegate.Read(ctx)
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
{{ else if .Primitive.State.Entry }}
type replicationServer struct {
	delegate Delegate
}

func (s *replicationServer) Read(ctx context.Context, key string) (*gossip.Object, error) {
	entry, err := s.delegate.Read(ctx, key)
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

func (s *replicationServer) Update(ctx context.Context, object *gossip.Object) error {
	entry := &{{ template "type" .Primitive.State.Entry.Type }}{}
	err := proto.Unmarshal(object.Value, entry)
	if err != nil {
		return err
	}
	return s.delegate.Update(ctx, entry)
}

func (s *replicationServer) ReadAll(ctx context.Context, ch chan<- gossip.Object) error {
	entriesCh := make(chan {{ template "type" .Primitive.State.Entry.Type }})
	errCh := make(chan error)
	go func() {
		err := s.delegate.List(ctx, entriesCh)
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
{{ end }}

var _ ReplicationServer = &replicationServer{}