package html

import (
	"embed"
	"fmt"
	"html/template"
	"io"

	"github.com/qri-io/wnfs-go/base"
)

//go:embed templates
var templates embed.FS

func RenderNode(w io.Writer, nd base.Node) error {
	tmpl, err := nodeTemplate(nd)
	if err != nil {
		return err
	}
	return tmpl.Execute(w, nd)
}

func nodeTemplate(n base.Node) (*template.Template, error) {
	return template.ParseFS(templates, fmt.Sprintf("templates/%s.html", n.Type().String()))
}
