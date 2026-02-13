package redlock

import (
	"github.com/google/uuid"
)

// newFencingToken generates a new UUID fencing token.
func newFencingToken() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
