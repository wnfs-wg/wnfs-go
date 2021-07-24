package ipfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	config "github.com/ipfs/go-ipfs-config"
	"github.com/ipfs/go-ipfs/assets"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/plugin/loader"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
)

const (
	nBitsForKeypair = 2048
)

var errRepoExists = errors.New(`ipfs configuration file already exists!
Reinitializing would overwrite your keys.
`)
var errRepoLock = errors.New(`this repo is currently being accessed by another process
This could mean that an ipfs daemon is running. please stop it before running this command
`)

// InitRepo is a more specific version of the init command: github.com/ipfs/go-ipfs/cmd/ipfs/init.go
// it's adapted to let qri initialize a repo. This func should be maintained to reflect the
// ipfs master branch.
func InitRepo(repoPath, configPath string) error {
	// if daemonLocked, err := fsrepo.LockedByOtherProcess(repoPath); err != nil {
	// 	return err
	// } else if daemonLocked {
	// 	return errRepoLock
	// }

	var conf *config.Config
	if configPath != "" {
		confFile, err := os.Open(configPath)
		if err != nil {
			return fmt.Errorf("error opening configuration file: %s", err.Error())
		}
		conf = &config.Config{}
		if err := json.NewDecoder(confFile).Decode(conf); err != nil {
			// res.SetError(err, cmds.ErrNormal)
			return fmt.Errorf("invalid configuration file: %s", configPath)
		}
	}

	if err := LoadIPFSPluginsOnce(repoPath); err != nil {
		return err
	}

	if err := doInit(ioutil.Discard, repoPath, false, nBitsForKeypair, "", conf); err != nil {
		return err
	}

	return nil
}

func applyProfiles(conf *config.Config, profiles string) error {
	if profiles == "" {
		return nil
	}

	for _, profile := range strings.Split(profiles, ",") {
		transformer, ok := config.Profiles[profile]
		if !ok {
			return fmt.Errorf("invalid configuration profile: %s", profile)
		}

		if err := transformer.Transform(conf); err != nil {
			return err
		}
	}
	return nil
}

func doInit(out io.Writer, repoRoot string, empty bool, nBitsForKeypair int, confProfiles string, conf *config.Config) error {

	if err := checkWriteable(repoRoot); err != nil {
		return err
	}

	if fsrepo.IsInitialized(repoRoot) {
		return errRepoExists
	}
	if _, err := fmt.Fprintf(out, "initializing IPFS node at %s\n", repoRoot); err != nil {
		return err
	}

	if conf == nil {
		var err error
		conf, err = config.Init(out, nBitsForKeypair)
		if err != nil {
			return err
		}
		// qri uses different default addresses
		conf.Addresses = config.Addresses{
			Swarm: []string{
				"/ip4/0.0.0.0/tcp/0",
				"/ip6/::/tcp/0",
				"/ip4/0.0.0.0/udp/0/quic",
				"/ip6/::/udp/0/quic",
			},
			Announce:   []string{},
			NoAnnounce: []string{},
			API: config.Strings{
				"/ip4/127.0.0.1/tcp/0",
			},
			Gateway: config.Strings{
				"/ip4/127.0.0.1/tcp/0",
			},
		}
	}

	if err := applyProfiles(conf, confProfiles); err != nil {
		return err
	}

	if err := fsrepo.Init(repoRoot, conf); err != nil {
		return err
	}

	if !empty {
		if err := addDefaultAssets(out, repoRoot); err != nil {
			return err
		}
	}

	return nil
}

func checkWriteable(dir string) error {
	_, err := os.Stat(dir)
	if err == nil {
		// dir exists, make sure we can write to it
		testfile := path.Join(dir, "test")
		fi, err := os.Create(testfile)
		if err != nil {
			if os.IsPermission(err) {
				return fmt.Errorf("%s is not writeable by the current user", dir)
			}
			return fmt.Errorf("unexpected error while checking writeablility of repo root: %s", err)
		}
		fi.Close()
		return os.Remove(testfile)
	}

	if os.IsNotExist(err) {
		// dir doesnt exist, check that we can create it
		return os.Mkdir(dir, 0775)
	}

	if os.IsPermission(err) {
		return fmt.Errorf("cannot write to %s, incorrect permissions", err)
	}

	return err
}

func addDefaultAssets(out io.Writer, repoRoot string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, err := fsrepo.Open(repoRoot)
	if err != nil { // NB: repo is owned by the node
		return err
	}

	nd, err := core.NewNode(ctx, &core.BuildCfg{Repo: r})
	if err != nil {
		return err
	}
	defer nd.Close()

	dkey, err := assets.SeedInitDocs(nd)
	if err != nil {
		return fmt.Errorf("init: seeding init docs failed: %s", err)
	}
	// log.Debugf("init: seeded init docs %s", dkey)

	if _, err = fmt.Fprintf(out, "to get started, enter:\n"); err != nil {
		return err
	}

	_, err = fmt.Fprintf(out, "\n\tipfs cat /ipfs/%s/readme\n\n", dkey)
	return err
}

var (
	pluginLoadLock  sync.Once
	pluginLoadError error
)

// LoadIPFSPluginsOnce runs IPFS plugin initialization.
// we need to load plugins before attempting to configure IPFS, flatfs is
// specified as part of the default IPFS configuration, but all flatfs
// code is loaded as a plugin.  ¯\_(ツ)_/¯
//
// This works without anything present in the /.ipfs/plugins/ directory b/c
// the default plugin set is complied into go-ipfs (and subsequently, the
// qri binary) by default
func LoadIPFSPluginsOnce(path string) error {
	body := func() {
		pluginLoadError = loadPlugins(path)
	}
	pluginLoadLock.Do(body)
	return pluginLoadError
}

// loadPlugins loads & injects plugins from a given repo path. This needs to be
// called once per active process with a repo
// NB: this implies that changing repo locations requires a process restart
func loadPlugins(repoPath string) error {
	// check if repo is accessible before loading plugins
	pluginpath := filepath.Join(repoPath, "plugins")

	var plugins *loader.PluginLoader
	ok, err := checkPermissions(repoPath)
	if err != nil {
		return err
	}
	if !ok {
		pluginpath = ""
	}
	plugins, err = loader.NewPluginLoader(pluginpath)
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	return nil
}

func checkPermissions(path string) (bool, error) {
	_, err := os.Open(path)
	if os.IsNotExist(err) {
		// repo does not exist yet - don't load plugins, but also don't fail
		return false, nil
	}
	if os.IsPermission(err) {
		// repo is not accessible. error out.
		return false, fmt.Errorf("error opening repository at %s: permission denied", path)
	}

	return true, nil
}
