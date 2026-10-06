package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// FileBackend keeps every object in one JSON file. After each change it
// rewrites the whole file, which is simple and fine for a small cluster.
//
// The file lists the objects of each kind:
//
//	{"pods": [{...}, {...}], "nodes": [{...}], ...}
type FileBackend struct {
	path    string
	objects map[string]map[string]json.RawMessage // by kind, then by name
}

// OpenFile returns a FileBackend that saves to path, starting with what's in
// the file if it already exists.
func OpenFile(path string) (*FileBackend, error) {
	f := &FileBackend{path: path, objects: make(map[string]map[string]json.RawMessage)}

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		// First start: create the folder now, so the first save can't fail on it.
		return f, os.MkdirAll(filepath.Dir(path), 0o755)
	}
	if err != nil {
		return nil, err
	}

	var lists map[string][]json.RawMessage
	err = json.Unmarshal(data, &lists)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	for kind, list := range lists {
		f.objects[kind] = make(map[string]json.RawMessage)
		for _, data := range list {
			// Every object has a "name" field, and some a "namespace". Read
			// just those, to know the object's key.
			var obj struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			}
			err := json.Unmarshal(data, &obj)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", path, err)
			}
			key := obj.Name
			if obj.Namespace != "" {
				key = api.Key(obj.Namespace, obj.Name)
			}
			f.objects[kind][key] = data
		}
	}
	return f, nil
}

// LoadAll returns every object in the file.
//
// In the file, objects are listed rather than keyed, and their key is worked
// out from their name and namespace when the file is read.
func (f *FileBackend) LoadAll() (map[string]map[string][]byte, error) {
	all := make(map[string]map[string][]byte)
	for kind, objects := range f.objects {
		all[kind] = make(map[string][]byte)
		for name, data := range objects {
			all[kind][name] = data
		}
	}
	return all, nil
}

// Put saves one object and rewrites the file.
func (f *FileBackend) Put(kind, name string, data []byte) error {
	if f.objects[kind] == nil {
		f.objects[kind] = make(map[string]json.RawMessage)
	}
	f.objects[kind][name] = data
	return f.write()
}

// Delete removes one object and rewrites the file.
func (f *FileBackend) Delete(kind, name string) error {
	delete(f.objects[kind], name)
	return f.write()
}

// Close does nothing: the file is already up to date after every change.
func (f *FileBackend) Close() error {
	return nil
}

// write saves every object to the file.
func (f *FileBackend) write() error {
	lists := make(map[string][]json.RawMessage)
	for kind, objects := range f.objects {
		lists[kind] = []json.RawMessage{} // so an empty kind is saved as [], not null
		for _, name := range slices.Sorted(maps.Keys(objects)) {
			lists[kind] = append(lists[kind], objects[name])
		}
	}

	data, err := json.MarshalIndent(lists, "", "  ")
	if err != nil {
		return err
	}

	// Write to a temporary file first, then rename it over the real one.
	// A rename is all-or-nothing, so a crash halfway through a write can never
	// leave a half-written file behind.
	tmp := f.path + ".tmp"
	err = os.WriteFile(tmp, data, 0o644)
	if err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}
