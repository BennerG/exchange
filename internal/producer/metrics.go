package producer

import "github.com/prometheus/client_golang/prometheus"

var (
	orderLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "exchange_producer_order_submit_duration_seconds",
		Help:    "Time to validate and publish a submitted order.",
		Buckets: prometheus.DefBuckets,
	})

	ordersTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "exchange_producer_orders_total",
		Help: "Total orders received, labeled by outcome.",
	}, []string{"outcome"})
)

func init() {
	prometheus.MustRegister(orderLatency, ordersTotal)
}
