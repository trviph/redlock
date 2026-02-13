package redlock

import (
	"crypto/sha1"
	"encoding/hex"
)

func init() {
	// Calculate SHA1 sums for Lua scripts
	shaAcquireOrExtend = sha1Sum(scriptAcquireOrExtend)
	shaExtend = sha1Sum(scriptExtend)
	shaRelease = sha1Sum(scriptRelease)
}

func sha1Sum(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}
