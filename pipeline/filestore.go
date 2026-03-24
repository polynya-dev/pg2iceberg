package pipeline

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hasyimibhar/pg2iceberg/config"
	"gopkg.in/yaml.v3"
)

// FileStore implements PipelineStore using a directory of YAML files.
// Each pipeline config is stored as {id}.yaml, using the same format
// as the single-tenant config file.
type FileStore struct {
	dir string
}

func NewFileStore(dir string) *FileStore {
	return &FileStore{dir: dir}
}

func (s *FileStore) Save(id string, cfg *config.Config) error {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("create store dir: %w", err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	path := filepath.Join(s.dir, id+".yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func (s *FileStore) Delete(id string) error {
	path := filepath.Join(s.dir, id+".yaml")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove config: %w", err)
	}
	return nil
}

func (s *FileStore) Load(id string) (*config.Config, error) {
	path := filepath.Join(s.dir, id+".yaml")
	return config.Load(path)
}

func (s *FileStore) List() (map[string]*config.Config, error) {
	entries, err := os.ReadDir(s.dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read store dir: %w", err)
	}

	result := make(map[string]*config.Config)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".yaml")
		cfg, err := s.Load(id)
		if err != nil {
			continue // skip invalid configs
		}
		result[id] = cfg
	}
	return result, nil
}
