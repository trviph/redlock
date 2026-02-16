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

// ReleaseStatus contains the result of a release operation.
type ReleaseStatus struct {
	TotalLocks    int
	SuccessCount  int
	QuorumReached bool
}

// quorum returns the number of instances required for a quorum.
func quorum(total int) int {
	return total/2 + 1
}
