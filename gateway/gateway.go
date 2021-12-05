package gateway

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log"
	"github.com/labstack/echo/v4"
	"github.com/qri-io/wnfs-go"
	"github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/fsdiff"
)

var log = golog.Logger("gateway")

func init() {
	golog.SetLogLevel("gateway", "info")
}

type Server struct {
	Factory wnfs.Factory
}

func (s *Server) Serve(addr string) error {
	e := echo.New()
	e.HideBanner = true
	e.GET("/:cid", s.HandleIndex)
	e.GET("/:cid/*", s.HandleIndex)
	e.GET("/history/:cid/*", s.HandleHistory)
	e.GET("/diff/:cid/*", s.HandleDiff)
	return e.Start(addr)
}

func (s *Server) HandleIndex(e echo.Context) error {
	ctx := e.Request().Context()
	n, err := s.open(ctx, e)
	if err != nil {
		return err
	}

	switch n.Type() {
	case base.NTFile:
		if _, err = io.Copy(e.Response(), n); err != nil {
			log.Errorw("writing response error", "err", err)
			return err
		}
	case base.NTDataFile:
		e.Response().Header().Set("Content-Type", "application/json")
		if _, err = io.Copy(e.Response(), n); err != nil {
			log.Errorw("writing response error", "err", err)
			return err
		}
	case base.NTDir:
		if err = RenderIndex(e.Response(), e.Request().URL.Path, n); err != nil {
			log.Errorw("rendering node error", "err", err)
			return err
		}
	}

	return nil
}

func (s *Server) HandleHistory(e echo.Context) error {
	ctx := e.Request().Context()
	n, err := s.open(ctx, e)
	if err != nil {
		return err
	}

	hist, err := n.History(ctx, 100)
	if err != nil {
		log.Errorw("history", "err", err)
		return err
	}

	if err = RenderHistory(e.Response(), e.Request().URL.Path, n, hist); err != nil {
		log.Errorw("rendering history error", "err", err)
		return err
	}

	return nil
}

func (s *Server) HandleDiff(e echo.Context) error {
	ctx := e.Request().Context()
	idstr, path := e.Param("cid"), e.Param("*")
	log.Infow("open", "cid", idstr, "path", path)

	id, err := cid.Parse(idstr)
	if err != nil {
		log.Infow("parsing input CID", "cidstr", idstr, "err", err)
		return err
	}

	fs, err := s.Factory.Load(ctx, id)
	if err != nil {
		log.Errorw("loading FS", "cid", idstr, "err", err)
		return err
	}

	n, err := fs.Open(path)
	if err != nil {
		return err
	}

	entries, err := fs.History(context.TODO(), ".", 2)
	if err != nil {
		return err
	}
	if len(entries) < 2 {
		return fmt.Errorf("no history")
	}

	prev, err := s.Factory.Load(ctx, entries[1].Cid)
	if err != nil {
		log.Errorw("loading prior entry", "err", err)
		return err
	}

	diff, err := fsdiff.Unix(path, path, prev, fs)
	if err != nil {
		log.Errorw("constructing diff", "err", err)
		return err
	}

	if err = RenderDiffs(e.Response(), e.Request().URL.Path, n.(wnfs.Node), diff); err != nil {
		log.Errorw("rendering history error", "err", err)
		return err
	}

	return nil
}

func (s *Server) open(ctx context.Context, e echo.Context) (wnfs.Node, error) {
	idstr, path := e.Param("cid"), e.Param("*")
	log.Infow("open", "cid", idstr, "path", path)

	id, err := cid.Parse(idstr)
	if err != nil {
		log.Infow("parsing input CID", "cidstr", idstr, "err", err)
		return nil, err
	}

	fs, err := s.Factory.Load(ctx, id)
	if err != nil {
		log.Errorw("loading FS", "cid", idstr, "err", err)
		return nil, err
	}

	f, err := fs.Open(path)
	if err != nil {
		log.Infow("opening path", "cidstr", idstr, "path", path, "err", err)
		return nil, err
	}

	return f.(base.Node), nil
}
