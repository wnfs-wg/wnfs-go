package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	humanize "github.com/dustin/go-humanize"
	golog "github.com/ipfs/go-log"
	wnfs "github.com/qri-io/wnfs-go"
	fsdiff "github.com/qri-io/wnfs-go/fsdiff"
	cli "github.com/urfave/cli/v2"
)

func init() {
	if lvl := os.Getenv("WNFS_LOGGING"); lvl != "" {
		golog.SetLogLevel("wnfs", lvl)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// var (
	// 	fs         wnfs.WNFS
	// 	store      mdstore.MerkleDagStore
	// 	state      *Repo
	// 	updateRepo func()
	// )

	var repo *Repo

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
				Usage:   "print verbose output",
			},
		},
		Before: func(c *cli.Context) (err error) {
			if c.Bool("verbose") {
				golog.SetLogLevel("wnfs", "debug")
			}

			repo, err = OpenRepo(ctx)
			return err
		},
		Commands: []*cli.Command{
			{
				Name:  "mkdir",
				Usage: "create a directory",
				Action: func(c *cli.Context) error {
					defer repo.Close()
					fs := repo.WNFS()
					return fs.Mkdir(c.Args().Get(0), wnfs.MutationOptions{
						Commit: true,
					})
				},
			},
			{
				Name:  "cat",
				Usage: "cat a file",
				Action: func(c *cli.Context) error {
					fs := repo.WNFS()
					data, err := fs.Cat(c.Args().Get(0))
					if err != nil {
						return err
					}
					_, err = os.Stdout.Write(data)
					return err
				},
			},
			{
				Name:    "write",
				Aliases: []string{"add"},
				Usage:   "add a file to wnfs",
				Action: func(c *cli.Context) error {
					path := c.Args().Get(0)
					file := c.Args().Get(1)
					f, err := os.Open(file)
					if err != nil {
						return err
					}

					defer repo.Close()
					fs := repo.WNFS()
					return fs.Write(path, f, wnfs.MutationOptions{
						Commit: true,
					})
				},
			},
			{
				Name:    "copy",
				Aliases: []string{"cp"},
				Usage:   "copy a file or directory into wnfs",
				Action: func(c *cli.Context) error {
					wnfsPath := c.Args().Get(0)
					localPath, err := filepath.Abs(c.Args().Get(1))
					if err != nil {
						return err
					}

					localFS := os.DirFS(filepath.Dir(localPath))
					path := filepath.Base(localPath)

					defer repo.Close()
					fs := repo.WNFS()
					return fs.Cp(wnfsPath, path, localFS, wnfs.MutationOptions{
						Commit: true,
					})
				},
			},
			{
				Name:  "ls",
				Usage: "list the contents of a directory",
				Action: func(c *cli.Context) error {
					fs := repo.WNFS()
					entries, err := fs.Ls(c.Args().Get(0))
					if err != nil {
						return err
					}

					for _, entry := range entries {
						fmt.Println(entry.Name())
					}
					return nil
				},
			},
			{
				Name:    "log",
				Aliases: []string{"history"},
				Usage:   "show the history of a path",
				Action: func(c *cli.Context) error {
					fs := repo.WNFS()
					entries, err := fs.History(c.Args().Get(0), -1)
					if err != nil {
						return err
					}

					fmt.Println("date\tsize\tcid\tkey\tprivate name")
					for _, entry := range entries {
						ts := time.Unix(entry.Metadata.UnixMeta.Mtime, 0)
						fmt.Printf("%s\t%s\t%s\t%s\t%s\n", ts.Format(time.RFC3339), humanize.Bytes(uint64(entry.Size)), entry.Cid, entry.Key, entry.PrivateName)
					}
					return nil
				},
			},
			{
				Name:  "rm",
				Usage: "remove files and directories",
				Action: func(c *cli.Context) error {
					defer repo.Close()
					fs := repo.WNFS()
					return fs.Rm(c.Args().Get(0), wnfs.MutationOptions{
						Commit: true,
					})
				},
			},
			{
				Name:  "tree",
				Usage: "show a tree rooted at a given path",
				Action: func(c *cli.Context) error {
					path := c.Args().Get(0)
					fs := repo.WNFS()
					// TODO(b5): can't yet create tree from wnfs root.
					// for now replace empty string with "public"
					if path == "" {
						path = "."
					}

					s, err := treeString(fs, path)
					if err != nil {
						return err
					}

					os.Stdout.Write([]byte(s))
					return nil
				},
			},
			{
				Name:  "diff",
				Usage: "",
				Action: func(c *cli.Context) error {
					cmdCtx, cancel := context.WithCancel(ctx)
					defer cancel()
					fs := repo.WNFS()

					entries, err := fs.History(".", 2)
					if err != nil {
						return err
					}
					if len(entries) < 2 {
						fmt.Println("no history")
						return nil
					}

					key := &wnfs.Key{}
					if err := key.Decode(entries[1].Key); err != nil {
						return err
					}

					prev, err := wnfs.FromCID(cmdCtx, repo.DagStore(), repo.RatchetStore(), entries[1].Cid, *key, wnfs.PrivateName(entries[1].PrivateName))
					if err != nil {
						errExit("error: opening previous WNFS %s:\n%s\n", entries[1].Cid, err.Error())
					}

					diff, err := fsdiff.Unix("", "", fs, prev)
					if err != nil {
						errExit("error: constructing diff: %s", err)
					}

					fmt.Println(fsdiff.PrettyPrintFileDiffs(diff))
					return nil
				},
			},
			{
				Name:  "merge",
				Usage: "",
				Action: func(c *cli.Context) error {
					a := repo.WNFS()
					cmdCtx, cancel := context.WithCancel(ctx)
					defer cancel()

					path := c.Args().Get(0)
					if filepath.Base(path) != repoDirname {
						path = filepath.Join(path, repoDirname)
					}
					fmt.Printf("reading wnfs repo from %q ...", path)
					bRepo, err := OpenRepoPath(cmdCtx, path)
					if err != nil {
						return err
					}
					b := bRepo.WNFS()
					fmt.Printf("done\n")

					defer repo.Close()
					_, err = wnfs.Merge(a, b)
					return err
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		errExit(err.Error())
	}
}

func errExit(msg string, v ...interface{}) {
	fmt.Printf(msg, v...)
	os.Exit(1)
}
