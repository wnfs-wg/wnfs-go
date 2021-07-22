package ipfs

import (
	"context"
	"fmt"

	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	migrate "github.com/ipfs/go-ipfs/repo/fsrepo/migrations"
)

const configFilename = "config"

// ErrNeedMigration indicates a migration must be run before qipfs can be used
var ErrNeedMigration = fmt.Errorf(`ipfs: need datastore migration`)

// Migrate runs an IPFS fsrepo migration
func Migrate(ctx context.Context, ipfsDir string) error {
	const httpUserAgent = "go-ipfs"

	fetchDistPath := migrate.GetDistPathEnv(migrate.CurrentIpfsDist)
	f := migrate.NewHttpFetcher(fetchDistPath, "", httpUserAgent, 0)
	err := migrate.RunMigration(ctx, f, fsrepo.RepoVersion, ipfsDir, false)
	if err != nil {
		fmt.Println("The migrations of fs-repo failed:")
		fmt.Printf("  %s\n", err)
		fmt.Println("If you think this is a bug, please file an issue and include this whole log output.")
		fmt.Println("  https://github.com/ipfs/fs-repo-migrations")
		return err
	}
	return nil
}
