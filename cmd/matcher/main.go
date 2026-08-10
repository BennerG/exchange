package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/BennerG/exchange/internal/consumer"
	"github.com/BennerG/exchange/internal/metrics"
	"github.com/BennerG/exchange/internal/store"
)

func main() {
	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	go func() {
		addr := envOr("METRICS_ADDR", ":2113")
		if err := metrics.Serve(addr); err != nil {
			log.Error().Err(err).Msg("metrics server error")
		}
	}()

	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	groupID := envOr("GROUP_ID", "exchange-matcher")
	transactionalID := envOr("TRANSACTIONAL_ID", "exchange-matcher-txn")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub, err := consumer.NewKafkaPublisher(brokers, transactionalID)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kafka publisher")
	}
	defer pub.Close()

	dlqPub, err := consumer.NewKafkaDeadLetterPublisher(brokers)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create dead letter publisher")
	}
	defer dlqPub.Close()

	connString := envOr("DATABASE_URL", "postgresql://exchange:exchange@localhost:5432/exchange?sslmode=disable")

	pendingStore, err := store.NewPostgresStore(ctx, connString)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create postgres store")
	}
	defer pendingStore.Close()

	retryingStore := store.NewRetryingPendingOrderStore(pendingStore, 3, 200*time.Millisecond)

	matcher, err := consumer.NewMatcher(ctx, retryingStore)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to reconstruct matcher from pending orders")
	}
	handler := consumer.NewTransactionalGroupHandler(matcher, pub, dlqPub, groupID)
	reconciler := consumer.NewReconciler(matcher.Snapshot, retryingStore, 30*time.Second)
	go reconciler.Run(ctx)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create consumer group")
	}
	defer group.Close()

	go func() {
		for err := range group.Errors() {
			log.Error().Err(err).Msg("consumer group error")
		}
	}()

	go func() {
		for {
			if err := group.Consume(ctx, []string{"orders"}, handler); err != nil {
				log.Error().Err(err).Msg("consumer group session ended")
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Info().Str("group_id", groupID).Msg("matcher consuming")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info().Msg("matcher shutting down")
	cancel()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
