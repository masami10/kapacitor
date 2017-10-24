package sideload

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/ghodss/yaml"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/pkg/errors"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic

	Error(msg string, err error)
}

type Service struct {
	diag Diagnostic

	mu      sync.Mutex
	sources []*source
}

func NewService(d Diagnostic) *Service {
	return &Service{
		diag: d,
	}
}

func (s *Service) Open() error {
	return nil
}
func (s *Service) Close() error {
	return nil
}

//  TODO
// 1. Add integration tests
// 2. Use cache for sources as well so duplicate sources are not created
// 3. Figure out how to do type safety possibly require default value.

func (s *Service) Source(dir string) (Source, error) {
	src := &source{
		s:   s,
		dir: dir,
	}
	err := src.updateCache()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.sources = append(s.sources, src)
	s.mu.Unlock()
	return src, nil
}

func (s *Service) removeSource(src *source) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.sources {
		if s.sources[i] == src {
			s.sources = append(s.sources[:i], s.sources[i+1:]...)
			break
		}
	}
}

type Source interface {
	Lookup(order []string, key string) interface{}
	Close()
}

type source struct {
	s     *Service
	dir   string
	mu    sync.RWMutex
	cache map[string]map[string]interface{}
}

func (s *source) Close() {
	s.s.removeSource(s)
}

func (s *source) updateCache() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache = make(map[string]map[string]interface{})
	err := filepath.Walk(s.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		values, err := readValues(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(s.dir, path)
		if err != nil {
			return err
		}
		s.cache[rel] = values
		return nil
	})
	return errors.Wrapf(err, "failed to update sideload cache for source %q", s.dir)
}

func (s *source) Lookup(order []string, key string) (value interface{}) {
	key = filepath.Clean(key)

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, o := range order {
		values, ok := s.cache[o]
		if !ok {
			continue
		}
		v, ok := values[key]
		if !ok {
			continue
		}
		value = v
		break
	}
	return
}

func readValues(p string) (map[string]interface{}, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open values file %q", p)
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read values file %q", p)
	}

	values := make(map[string]interface{})
	ext := filepath.Ext(p)
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &values); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal yaml values %q", p)
		}
	case ".json":
		if err := json.Unmarshal(data, &values); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal json values %q", p)
		}
	}
	return values, nil
}
