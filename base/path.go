package base

import "strings"

type Path []string

func MustPath(posix string) Path {
	p, err := NewPath(posix)
	if err != nil {
		panic(err)
	}
	return p
}

func NewPath(posix string) (Path, error) {
	posix = strings.TrimPrefix(posix, "/")
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
