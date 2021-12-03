package gateway

import (
	"github.com/labstack/echo/v4"
	"github.com/qri-io/wnfs-go"
	"github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/gateway/html"
)

type Server struct {
	FS wnfs.WNFS
}

func (s *Server) Serve(addr string) error {
	e := echo.New()
	e.GET("/", s.HandleGatewayHTML())
	return e.Start(addr)
}

func (s *Server) HandleGatewayHTML(e echo.Context) error {
	s.FS.Open(e.Request().URL().Path)

return html.RenderNode(e.Response(), nd base.Node) {

	return nil
}
