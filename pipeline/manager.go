package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/hasyimibhar/pg2iceberg/config"
)

// PipelineInfo exposes the state of a pipeline for API consumers.
type PipelineInfo struct {
	ID     string         `json:"id"`
	Status Status         `json:"status"`
	Error  string         `json:"error,omitempty"`
	Config *config.Config `json:"config"`
}

// Manager manages the lifecycle of multiple pipelines.
type Manager struct {
	ctx       context.Context // long-lived context for all pipelines
	pipelines map[string]*Pipeline
	store     PipelineStore
	ch        *ClickHouseProvisioner // optional, nil if not configured
	mu        sync.RWMutex
}

func NewManager(ctx context.Context, store PipelineStore) *Manager {
	return &Manager{
		ctx:       ctx,
		pipelines: make(map[string]*Pipeline),
		store:     store,
	}
}

// SetClickHouse enables automatic ClickHouse database provisioning.
func (m *Manager) SetClickHouse(ch *ClickHouseProvisioner) {
	m.ch = ch
}

// Create validates, persists, and starts a new pipeline.
// The pipeline runs under the Manager's long-lived context, not the request context.
func (m *Manager) Create(_ context.Context, id string, cfg *config.Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.pipelines[id]; exists {
		return fmt.Errorf("pipeline %q already exists", id)
	}

	// Persist config before starting.
	if err := m.store.Save(id, cfg); err != nil {
		return fmt.Errorf("persist config: %w", err)
	}

	// Provision ClickHouse database for this namespace.
	if m.ch != nil {
		if err := m.ch.EnsureDatabase(cfg.Sink.Namespace); err != nil {
			log.Printf("[manager] warning: clickhouse provisioning failed for %q: %v", id, err)
			// Non-fatal: pipeline can still replicate, ClickHouse can be set up later.
		} else {
			log.Printf("[manager] provisioned clickhouse database %q", cfg.Sink.Namespace)
		}
	}

	p := NewPipeline(id, cfg)
	if err := p.Start(m.ctx); err != nil {
		m.store.Delete(id) // rollback
		return fmt.Errorf("start pipeline: %w", err)
	}

	m.pipelines[id] = p
	log.Printf("[manager] created pipeline %q", id)
	return nil
}

// Delete stops and removes a pipeline.
func (m *Manager) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, exists := m.pipelines[id]
	if !exists {
		return fmt.Errorf("pipeline %q not found", id)
	}

	if err := p.Stop(); err != nil {
		log.Printf("[manager] error stopping pipeline %q: %v", id, err)
	}

	if err := m.store.Delete(id); err != nil {
		return fmt.Errorf("delete config: %w", err)
	}

	delete(m.pipelines, id)
	log.Printf("[manager] deleted pipeline %q", id)
	return nil
}

// Get returns info about a single pipeline.
func (m *Manager) Get(id string) (*PipelineInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	p, exists := m.pipelines[id]
	if !exists {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}

	return pipelineToInfo(p), nil
}

// List returns info about all managed pipelines.
func (m *Manager) List() []PipelineInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	infos := make([]PipelineInfo, 0, len(m.pipelines))
	for _, p := range m.pipelines {
		infos = append(infos, *pipelineToInfo(p))
	}
	return infos
}

// AddTable adds a table to a running pipeline.
func (m *Manager) AddTable(_ context.Context, pipelineID, tableName string) error {
	m.mu.RLock()
	p, exists := m.pipelines[pipelineID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("pipeline %q not found", pipelineID)
	}
	return p.AddTable(m.ctx, tableName)
}

// RemoveTable removes a table from a running pipeline.
func (m *Manager) RemoveTable(_ context.Context, pipelineID, tableName string) error {
	m.mu.RLock()
	p, exists := m.pipelines[pipelineID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("pipeline %q not found", pipelineID)
	}
	return p.RemoveTable(m.ctx, tableName)
}

// RestoreAll loads all persisted pipeline configs and starts them.
// Called on server startup to resume pipelines from a previous run.
func (m *Manager) RestoreAll() error {
	configs, err := m.store.List()
	if err != nil {
		return fmt.Errorf("list configs: %w", err)
	}

	for id, cfg := range configs {
		p := NewPipeline(id, cfg)
		if err := p.Start(m.ctx); err != nil {
			log.Printf("[manager] failed to restore pipeline %q: %v", id, err)
			continue
		}
		m.mu.Lock()
		m.pipelines[id] = p
		m.mu.Unlock()
		log.Printf("[manager] restored pipeline %q", id)
	}
	return nil
}

// StopAll gracefully stops all pipelines. Called on server shutdown.
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, p := range m.pipelines {
		if err := p.Stop(); err != nil {
			log.Printf("[manager] error stopping pipeline %q: %v", id, err)
		}
	}
}

func pipelineToInfo(p *Pipeline) *PipelineInfo {
	status, err := p.Status()
	info := &PipelineInfo{
		ID:     p.ID(),
		Status: status,
		Config: p.Config(),
	}
	if err != nil {
		info.Error = err.Error()
	}
	return info
}
