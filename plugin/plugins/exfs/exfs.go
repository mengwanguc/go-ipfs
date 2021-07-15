package exfs

import (
	"fmt"
	"path/filepath"
	"os"

	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"

	examples "github.com/ipfs/go-datastore/examples"
)

// Plugins is exported list of plugins that will be loaded
var Plugins = []plugin.Plugin{
	&exfsPlugin{},
}

type exfsPlugin struct{}

var _ plugin.PluginDatastore = (*exfsPlugin)(nil)

func (*exfsPlugin) Name() string {
	return "ds-exfs"
}

func (*exfsPlugin) Version() string {
	return "0.1.0"
}

func (*exfsPlugin) Init(_ *plugin.Environment) error {
	return nil
}

func (*exfsPlugin) DatastoreTypeName() string {
	return "exfs"
}

type datastoreConfig struct {
	path      string
}

// BadgerdsDatastoreConfig returns a configuration stub for a badger datastore
// from the given parameters
func (*exfsPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(params map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		var c datastoreConfig
		var ok bool

		c.path, ok = params["path"].(string)
		if !ok {
			return nil, fmt.Errorf("'path' field is missing or not a string")
		}

		return &c, nil
	}
}

func (c *datastoreConfig) DiskSpec() fsrepo.DiskSpec {
	return map[string]interface{}{
		"type":      "exfs",
		"path":      c.path,
	}
}

func (c *datastoreConfig) Create(path string) (repo.Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}
	err := os.MkdirAll(p, 0755)
	if err != nil {
		return nil, err
	}

	return examples.NewDatastore(p)
}
