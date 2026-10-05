package observability

import "strings"

// SanitizeForLog strips control characters and bounds length so request
// derived values (paths, error codes) can never forge log lines or bury
// sensitive free text. It never expands or echoes request bodies.
func SanitizeForLog(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 256 {
		value = value[:256]
	}
	var builder strings.Builder
	builder.Grow(len(value))
	for _, r := range value {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			builder.WriteByte(' ')
		case r < 0x20 || r == 0x7f:
			builder.WriteByte('?')
		default:
			builder.WriteRune(r)
		}
	}
	return builder.String()
}
