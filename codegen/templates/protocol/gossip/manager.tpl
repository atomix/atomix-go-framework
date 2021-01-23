{{- $serviceType := printf "%sServiceType" .Generator.Prefix }}

{{- define "type" }}{{ printf "%s.%s" .Package.Alias .Name }}{{ end }}

package {{ .Package.Name }}

import (
	"context"
	"time"
	{{- if .Primitive.State.Entry }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
	{{- end }}
)

const antiEntropyPeriod = time.Second

func newManager(client ReplicationClient, service Service) *serviceManager {
	return &serviceManager{
		client:  client,
		service: service,
		ticker:  time.NewTicker(antiEntropyPeriod),
	}
}

type serviceManager struct {
	client  ReplicationClient
	service Service
	ticker  *time.Ticker
}

func (m *serviceManager) start(ctx context.Context) error {
	if err := m.bootstrap(ctx); err != nil {
		return err
	}
	go m.runAntiEntropy()
	return nil
}

func (m *serviceManager) bootstrap(ctx context.Context) error {
    {{- if .Primitive.State.Value }}
    entry, err := m.client.Bootstrap(ctx)
    if err != nil {
        return err
    }
    if err := m.service.Delegate().Update(ctx, entry); err != nil {
        return err
    }
    {{- else if .Primitive.State.Entry }}
	entryCh := make(chan {{ template "type" .Primitive.State.Entry.Type }})
	if err := m.client.Bootstrap(ctx, entryCh); err != nil {
		return err
	}
	for entry := range entryCh {
		if err := m.service.Delegate().Update(ctx, &entry); err != nil {
			return err
		}
	}
    {{- end }}
	return nil
}

func (m *serviceManager) runAntiEntropy() {
	for range m.ticker.C {
		if err := m.advertise(context.Background()); err != nil {
			log.Errorf("Anti-entropy protocol failed: %v", err)
		}
	}
}

func (m *serviceManager) advertise(ctx context.Context) error {
    {{- if .Primitive.State.Value }}
    entry, err := m.service.Delegate().Read(context.Background())
    if err != nil {
        return err
    }
    if err := m.client.Advertise(context.Background(), entry); err != nil {
        return err
    }
    {{- else if .Primitive.State.Entry }}
	entryCh := make(chan {{ template "type" .Primitive.State.Entry.Type }})
	if err := m.service.Delegate().List(context.Background(), entryCh); err != nil {
		return err
	}
	for entry := range entryCh {
		if err := m.client.Advertise(context.Background(), &entry); err != nil {
			return err
		}
	}
    {{- end }}
	return nil
}

func (m *serviceManager) stop() {
	m.ticker.Stop()
}
