package gateway

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"path/filepath"

	"github.com/qri-io/wnfs-go/base"
)

//go:embed templates
var templates embed.FS

func RenderNode(w io.Writer, path string, nd base.Node) error {
	tmpl, err := nodeTemplate(nd)
	if err != nil {
		return err
	}
	return tmpl.Execute(w, map[string]interface{}{
		"Parent": parent(path),
		"Path":   path,
		"Node":   nd,
	})
}

func parent(path string) string {
	parent := filepath.Dir(path)
	if parent == "/" {
		return path
	}
	return parent
}

func nodeTemplate(n base.Node) (*template.Template, error) {
	return template.ParseFS(templates, fmt.Sprintf("templates/%s.html", n.Type().String()))
}
