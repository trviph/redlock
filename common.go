package redlock

import (
	"time"

	"github.com/google/uuid"
)

// closedChan is used to immediately unblock select statements
// for first attempts or when max retries are exceeded.
var closedChan = make(chan time.Time)

func init() {
	close(closedChan)
}

// newFencingToken generates a new UUID fencing token.
func newFencingToken() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
