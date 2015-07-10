package store

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

// FileCacheStore implements a super simple wrapper around RemoteStore
// to handle local file caching of metadata
type FileCacheStore struct {
	cachePath string
}

func NewFileCacheStore(cachePath string) *FileCacheStore {
	return &FileCacheStore{
		cachePath: cachePath,
	}
}

func (s FileCacheStore) cacheFile(name string, data []byte) error {
	path := path.Join(s.cachePath, name)
	dir := filepath.Dir(path)
	os.MkdirAll(dir, 0600)
	return ioutil.WriteFile(path+".json", data, 0600)
}

func (s FileCacheStore) useCachedFile(name string) ([]byte, error) {
	path := path.Join(s.cachePath, name+".json")
	return ioutil.ReadFile(path)
}

func (s FileCacheStore) Get(name string, size int64) ([]byte, error) {
	data, err := s.useCachedFile(name)
	if err == nil || data != nil {
		return data, nil
	}
	data, err = s.RemoteStore.GetMeta(name, size)
	if err != nil {
		return nil, err
	}
	s.cacheFile(name, data)
	return data, nil
}

func (s FileCacheStore) Set(name string, data []byte) error {
	err := os.MkdirAll(s.cachePath, 0700)
	if err != nil {
		return err
	}
	path := path.Join(s.cachePath, name+".json")
	return ioutil.WriteFile(name, data, 0600)
}
