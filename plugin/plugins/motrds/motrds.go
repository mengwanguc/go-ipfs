package motrds

import (
	"fmt"

	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"

        mds "github.com/mengwanguc/go-ds-motr/mds"
        mio "github.com/mengwanguc/go-ds-motr/mio"
)

// Plugins is exported list of plugins that will be loaded
var Plugins = []plugin.Plugin{
	&motrdsPlugin{},
}

type motrdsPlugin struct{}

var _ plugin.PluginDatastore = (*motrdsPlugin)(nil)

var motrds *mds.MotrDS = nil

func (*motrdsPlugin) Name() string {
	return "ds-motrds"
}

func (*motrdsPlugin) Version() string {
	return "0.1.0"
}

func (*motrdsPlugin) Init(_ *plugin.Environment) error {
	return nil
}

func (*motrdsPlugin) DatastoreTypeName() string {
	return "motrds"
}

type datastoreConfig struct {
	config      mio.Config
	indexID     string
}


// BadgerdsDatastoreConfig returns a configuration stub for a badger datastore
// from the given parameters
func (*motrdsPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(params map[string]interface{}) (fsrepo.DatastoreConfig, error) {

		localEP, ok := params["localEP"].(string)
		if !ok {
			return nil, fmt.Errorf("'LocalEP' field is missing or not a string")
		}

		haxEP, ok := params["haxEP"].(string)
		if !ok {
			return nil, fmt.Errorf("'HaxEP' field is missing or not a string")
		}

		profile, ok := params["profile"].(string)
		if !ok {
			return nil, fmt.Errorf("'LocalEP' field is missing or not a string")
		}

		procFid, ok := params["procFid"].(string)
		if !ok {
			return nil, fmt.Errorf("'ProcFid' field is missing or not a string")
		}

		indexID, ok := params["indexID"].(string)
		if !ok {
			return nil, fmt.Errorf("'ProcFid' field is missing or not a string")
		}

		return &datastoreConfig{
			config: mio.Config{
		        LocalEP:    localEP,
		        HaxEP:      haxEP,
		        Profile:    profile,
		        ProcFid:    procFid,
		        TraceOn:    false,
		        Verbose:    false,
		        ThreadsN:   1,
		    },
		    indexID: indexID,
		}, nil
	}
}

func (c *datastoreConfig) DiskSpec() fsrepo.DiskSpec {
	return map[string]interface{}{
		"type":      "motrds",
		"localEP":   c.config.LocalEP,
		"haxEP":     c.config.HaxEP,
		"profile":   c.config.Profile,
		"procFid":   c.config.ProcFid,
	}
}

func (c *datastoreConfig) Create(path string) (repo.Datastore, error) {
        fmt.Println("creating a new datastore")
	var err error
        if motrds == nil {
            motrds, err =  mds.Open(c.config, c.indexID)
            return motrds, err
        } else {
            return motrds, nil
        }
}
