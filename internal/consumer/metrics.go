package consumer

import "github.com/prometheus/client_golang/prometheus"

var (
	matchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "exchange_matcher_match_duration_seconds",
		Help:    "Time to compute a match against the order book.",
		Buckets: []float64{0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
	})

	fillsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "exchange_matcher_fills_total",
		Help: "Total fills produced by the matcher.",
	})

	transactionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "exchange_matcher_transactions_total",
		Help: "Total Kafka transactions attempted by the matcher, labeled by outcome.",
	}, []string{"outcome"})

	settleDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "exchange_settler_settle_duration_seconds",
		Help:    "Time to settle a fill into Postgres.",
		Buckets: prometheus.DefBuckets,
	})

	settlementsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "exchange_settler_settlements_total",
		Help: "Total settlement attempts, labeled by outcome.",
	}, []string{"outcome"})

	consumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "exchange_consumer_lag",
		Help: "Partition high water mark minus the last processed offset, labeled by topic.",
	}, []string{"topic"})

	dlqMessagesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "exchange_dlq_messages_total",
		Help: "Total messages routed to the dead letter queue, labeled by source topic.",
	}, []string{"topic"})
)

func init() {
	prometheus.MustRegister(
		matchDuration, fillsTotal, transactionsTotal,
		settleDuration, settlementsTotal,
		consumerLag, dlqMessagesTotal,
	)
}
