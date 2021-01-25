package value

import (
	"context"
	"time"
)

const antiEntropyPeriod = time.Second

func newManager(client ReplicationClient, service Service) *serviceManager {
	return &serviceManager{
		client:  client,
		service: service,
	}
}

type serviceManager struct {
	client  ReplicationClient
	service Service
	ticker  *time.Ticker
	cancel  context.CancelFunc
}

func (m *serviceManager) start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	if err := m.bootstrap(ctx); err != nil {
		log.Errorf("Failed to bootstrap service: %v", err)
	}
	m.runAntiEntropy(ctx)
}

func (m *serviceManager) bootstrap(ctx context.Context) error {
	entry, err := m.client.Bootstrap(ctx)
	if err != nil {
		return err
	}
	if err := m.service.Delegate().Update(ctx, entry); err != nil {
		return err
	}
	return nil
}

func (m *serviceManager) runAntiEntropy(ctx context.Context) {
	m.ticker = time.NewTicker(antiEntropyPeriod)
	for range m.ticker.C {
		if err := m.advertise(ctx); err != nil {
			log.Errorf("Anti-entropy protocol failed: %v", err)
		}
	}
}

func (m *serviceManager) advertise(ctx context.Context) error {
	entry, err := m.service.Delegate().Read(ctx)
	if err != nil {
		return err
	}
	if err := m.client.Advertise(context.Background(), entry); err != nil {
		return err
	}
	return nil
}

func (m *serviceManager) stop() {
	m.ticker.Stop()
	m.cancel()
}
