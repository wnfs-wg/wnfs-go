package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log"
	wnfs "github.com/qri-io/wnfs-go"
	wnipfs "github.com/qri-io/wnfs-go/ipfs"
	"github.com/qri-io/wnfs-go/mdstore"
	cli "github.com/urfave/cli/v2"
)

func open(ctx context.Context) (wnfs.WNFS, *ExternalState) {
	ipfsPath := os.Getenv("IPFS_PATH")
	if ipfsPath == "" {
		dir, err := configDirPath()
		if err != nil {
			errExit("error: getting configuration directory: %s\n", err)
		}
		ipfsPath = filepath.Join(dir, "ipfs")

		if _, err := os.Stat(filepath.Join(ipfsPath, "config")); os.IsNotExist(err) {
			if err := os.MkdirAll(ipfsPath, 0755); err != nil {
				errExit("error: creating ipfs repo: %s\n", err)
			}
			fmt.Printf("creating ipfs repo at %s ... ", ipfsPath)
			if err = wnipfs.InitRepo(ipfsPath, ""); err != nil {
				errExit("\nerror: %s", err)
			}
			fmt.Println("done")
		}
	}

	store, err := wnipfs.NewFilesystem(ctx, map[string]interface{}{
		"path": ipfsPath,
	})

	if err != nil {
		errExit("error: opening IPFS repo: %s\n", err)
	}

	statePath, err := ExternalStatePath()
	if err != nil {
		errExit("error: getting state path: %s\n", err)
	}
	state, err := LoadOrCreateExternalState(statePath)
	if err != nil {
		errExit("error: loading external state: %s\n", err)
	}

	var fs wnfs.WNFS
	if state.RootCID.Equals(cid.Cid{}) {
		fmt.Printf("creating new wnfs filesystem...")
		if fs, err = wnfs.NewEmptyFS(ctx, store); err != nil {
			errExit("error: creating empty WNFS: %s\n", err)
		}
		fmt.Println("done")
	} else {
		if fs, err = wnfs.FromCID(ctx, store, state.RootCID); err != nil {
			errExit("error: opening WNFS CID %s:\n%s\n", state.RootCID, err.Error())
		}
	}

	return fs, state
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		fs                  wnfs.WNFS
		state               *ExternalState
		updateExternalState func()
	)

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
				Usage:   "print verbose output",
			},
		},
		Before: func(c *cli.Context) error {
			if c.Bool("verbose") {
				golog.SetLogLevel("wnfs", "debug")
			}

			fs, state = open(ctx)
			updateExternalState = func() {
				state.RootCID = fs.(mdstore.DagNode).Cid()
				fmt.Printf("writing root cid: %s...", state.RootCID)
				if err := state.Write(); err != nil {
					errExit("error: writing external state: %s\n", err)
				}
				fmt.Println("done")
			}

			return nil
		},
		Commands: []*cli.Command{
			{
				Name:  "mkdir",
				Usage: "create a directory",
				Action: func(c *cli.Context) error {
					defer updateExternalState()
					return fs.Mkdir(c.Args().Get(0), wnfs.MutationOptions{
						Commit: true,
					})
				},
			},
			{
				Name:  "cat",
				Usage: "cat a file",
				Action: func(c *cli.Context) error {
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

					defer updateExternalState()
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

					defer updateExternalState()
					return fs.Cp(wnfsPath, path, localFS, wnfs.MutationOptions{
						Commit: true,
					})
				},
			},
			{
				Name:  "ls",
				Usage: "list the contents of a directory",
				Action: func(c *cli.Context) error {
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
					entries, err := fs.History(c.Args().Get(0), -1)
					if err != nil {
						return err
					}

					fmt.Println("date\tsize\tcid")
					for _, entry := range entries {
						ts := time.Unix(entry.Metadata.UnixMeta.Mtime, 0)
						fmt.Printf("%s\t%s\t%s\n", ts.Format(time.RFC3339), humanize.Bytes(uint64(entry.Size)), entry.Cid)
					}
					return nil
				},
			},
			{
				Name:  "rm",
				Usage: "remove files and directories",
				Action: func(c *cli.Context) error {
					defer updateExternalState()
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
					// TODO(b5): can't yet create tree from wnfs root.
					// for now replace empty string with "public"
					if path == "" {
						path = "public"
					}

					s, err := treeString(fs, path)
					if err != nil {
						return err
					}

					os.Stdout.Write([]byte(s))
					return nil
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
