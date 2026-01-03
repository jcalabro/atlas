package util

// Returns a pointer to the given object (useful for working with protobuf
// and ATProto APIs)
func Ptr[T any](t T) *T {
	return &t
}
