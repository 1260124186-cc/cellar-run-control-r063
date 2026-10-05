package operation

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"
)

// HeaderName carries the caller-supplied operation identifier on a request
// and echoes the authoritative identifier back on every response. A terminal
// retry reuses the same value so both attempts correlate to one operation.
const HeaderName = "X-Operation-Id"

var suppliedIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// NewID returns a unique identifier with a readable prefix, for example
// "op_9f2c1a...". It never fails: a time-based value is used when the random
// source is unavailable.
func NewID(prefix string) string {
	buffer := make([]byte, 10)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UTC().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(buffer)
}

// ResolveID returns the caller-supplied identifier when it is safe to log
// and matches the expected shape; otherwise it generates a fresh identifier.
// Rejecting malformed values keeps newline and control characters out of the
// operation log.
func ResolveID(supplied string) string {
	if suppliedIDPattern.MatchString(supplied) {
		return supplied
	}
	return NewID("op")
}
