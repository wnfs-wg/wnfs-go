package base

import "strings"

type Path []string

func NewPath(posix string) (Path, error) {
	return strings.Split(posix, "/"), nil
}

func (p Path) String() string {
	return strings.Join(p, "/")
}

func (p Path) Shift() (head string, ch Path) {
	switch len(p) {
	case 0:
		return "", nil
	case 1:
		return p[0], nil
	default:
		return p[0], p[1:]
	}
}
