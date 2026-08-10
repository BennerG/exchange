package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Serve starts a metrics HTTP server on addr and blocks. Run it in its
// own goroutine alongside a service's main work.
func Serve(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(addr, mux)
}
