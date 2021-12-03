package gateway

import (
	"io"

	"github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log"
	"github.com/labstack/echo/v4"
	"github.com/qri-io/wnfs-go"
	"github.com/qri-io/wnfs-go/base"
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
	e.GET("/:cid", s.HandleGateway)
	e.GET("/:cid/*", s.HandleGateway)
	return e.Start(addr)
}

func (s *Server) HandleGateway(e echo.Context) error {
	ctx := e.Request().Context()
	idstr, path := e.Param("cid"), e.Param("*")
	log.Infow("get", "cid", idstr, "path", path)

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

	f, err := fs.Open(path)
	if err != nil {
		log.Infow("opening path", "cidstr", idstr, "path", path, "err", err)
		return err
	}

	n := f.(base.Node)

	switch n.Type() {
	case base.NTFile:
		if _, err = io.Copy(e.Response(), n); err != nil {
			log.Errorw("writing response error", "cidstr", idstr, "path", path, "err", err)
			return err
		}
	case base.NTDataFile:
		e.Response().Header().Set("Content-Type", "application/json")
		if _, err = io.Copy(e.Response(), n); err != nil {
			log.Errorw("writing response error", "cidstr", idstr, "path", path, "err", err)
			return err
		}
	case base.NTDir:
		if err = RenderNode(e.Response(), e.Request().URL.Path, n); err != nil {
			log.Errorw("rendering node error", "cidstr", idstr, "path", path, "err", err)
			return err
		}
	}

	return nil
}
