package gateway

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/qri-io/wnfs-go"
	"github.com/qri-io/wnfs-go/base"
	"github.com/qri-io/wnfs-go/fsdiff"
)

//go:embed templates
var templates embed.FS

func RenderIndex(w io.Writer, path string, nd base.Node) error {
	tmpl, err := newTemplate(fmt.Sprintf("%s.html", nd.Type().String()))
	if err != nil {
		return err
	}
	return tmpl.Execute(w, map[string]interface{}{
		"View":    "Index",
		"Parent":  parent(path),
		"Path":    path,
		"Private": wnfs.NodeIsPrivate(nd),
		"Node":    nd,
	})
}

func RenderHistory(w io.Writer, path string, nd base.Node, hist []base.HistoryEntry) error {
	tmpl, err := newTemplate("history.html")
	if err != nil {
		return err
	}
	return tmpl.Execute(w, map[string]interface{}{
		"View":    "History",
		"Parent":  parent(path),
		"Path":    strings.TrimPrefix(path, "/history"),
		"Private": wnfs.NodeIsPrivate(nd),
		"Node":    nd,
		"History": hist,
	})
}

func RenderDiffs(w io.Writer, path string, nd base.Node, diffs []fsdiff.FileDiff) error {
	tmpl, err := newTemplate("diff.html")
	if err != nil {
		return err
	}
	return tmpl.Execute(w, map[string]interface{}{
		"View":    "Diff",
		"Parent":  parent(path),
		"Path":    strings.TrimPrefix(path, "/diff"),
		"Private": wnfs.NodeIsPrivate(nd),
		"Node":    nd,
		"Diffs":   diffs,
	})
}

func parent(path string) string {
	parent := filepath.Dir(path)
	if parent == "/" {
		return path
	}
	return parent
}

func newTemplate(name string) (*template.Template, error) {
	return template.New(name).Funcs(template.FuncMap{
		"Bytes": func(i int64) string {
			return humanize.Bytes(uint64(i))
		},
		"RelTimestamp": func(i int64) string {
			return humanize.Time(time.Unix(i, 0))
		},
		"Timestamp": func(i int64) string {
			return time.Unix(i, 0).Format(time.RFC3339)
		},
		"DiffHTML": func(file fsdiff.FileDiff) template.HTML {
			return template.HTML(fsdiff.HTMLPrintFileDiff(file))
		},
		"MetaHTML": func(v interface{}) template.HTML {
			str := strings.Builder{}
			switch x := v.(type) {
			case map[string]interface{}:
				str.WriteString("<table>\n")
				for key, val := range x {
					str.WriteString(fmt.Sprintf("<tr><td>%s</td><td>%s</td></tr>\n", key, val))
				}
				str.WriteString("</table>\n")
			case []interface{}:
				// TODO(b5)
			}

			return template.HTML(str.String())
		},
	}).ParseFS(templates, "templates/*.html")
}
