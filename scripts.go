package redlock

import (
	"context"
	"strings"

	redis "github.com/redis/go-redis/v9"
)

const (
	scriptAcquireOrExtend = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		elseif redis.call("set", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
			return 1
		else
			return 0
		end
	`

	scriptExtend = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	scriptRelease = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
)

var (
	shaAcquireOrExtend string
	shaExtend          string
	shaRelease         string
)

// runScript executes a Lua script using EVALSHA, falling back to EVAL if the script is not loaded.
func runScript(ctx context.Context, rcli redisClient, script, sha string, keys []string, args ...any) *redis.Cmd {
	cmd := rcli.EvalSha(ctx, sha, keys, args...)
	if err := cmd.Err(); err != nil && isNoScriptError(err) {
		return rcli.Eval(ctx, script, keys, args...)
	}
	return cmd
}

func isNoScriptError(err error) bool {
	return strings.HasPrefix(err.Error(), "NOSCRIPT")
}
