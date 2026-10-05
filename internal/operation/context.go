package operation

import "context"

type idContextKey struct{}

// WithID returns a context that carries the operation identifier. The
// identifier travels with a single request through the service and storage
// layers, so concurrent requests never share an operation number.
func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, idContextKey{}, id)
}

// IDFromContext returns the operation identifier attached to the context, or
// an empty string when the context carries none.
func IDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(idContextKey{}).(string)
	return id
}
