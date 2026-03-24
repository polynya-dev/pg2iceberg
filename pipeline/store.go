package pipeline

import "github.com/hasyimibhar/pg2iceberg/config"

// PipelineStore persists pipeline configurations across restarts.
type PipelineStore interface {
	Save(id string, cfg *config.Config) error
	Delete(id string) error
	Load(id string) (*config.Config, error)
	List() (map[string]*config.Config, error)
}
