// Package objectid validates client-supplied object identifiers.
//
// Object IDs become file names on storage nodes, so validation is the first
// line of defence against path traversal. The allowed alphabet contains no
// path separators, and "." / ".." are rejected explicitly.
package objectid

import (
	"errors"
	"fmt"
)

// MaxLen is the maximum length of an object ID in bytes.
const MaxLen = 200

// ErrInvalid is returned (wrapped) for every validation failure.
var ErrInvalid = errors.New("invalid object id")

// Validate reports whether id is a safe object identifier.
//
// Allowed: ASCII letters, digits, '.', '_', '-'. The first character must be a
// letter or digit, which also rules out hidden files and "."/"..".
func Validate(id string) error {
	if id == "" {
		return fmt.Errorf("%w: empty", ErrInvalid)
	}
	if len(id) > MaxLen {
		return fmt.Errorf("%w: longer than %d bytes", ErrInvalid, MaxLen)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case (c == '.' || c == '_' || c == '-') && i > 0:
		default:
			return fmt.Errorf("%w: illegal character %q at position %d", ErrInvalid, c, i)
		}
	}
	return nil
}
