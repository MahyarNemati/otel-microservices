package inventory_test

import "net/http"

// propagationHeader adapts http.Header to the OTel carrier interface, which is
// what otelhttp does internally; doing it by hand in the test keeps the test
// honest about what crosses the wire.
type propagationHeader http.Header

func (h propagationHeader) Get(key string) string { return http.Header(h).Get(key) }

func (h propagationHeader) Set(key, value string) { http.Header(h).Set(key, value) }

func (h propagationHeader) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}
