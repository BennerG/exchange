package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/BennerG/exchange/internal/consumer"
	"github.com/BennerG/exchange/internal/store"
)

func main() {
	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()

	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	groupID := envOr("GROUP_ID", "exchange-settler")
	connString := envOr("DATABASE_URL", "postgresql://exchange:exchange@localhost:5432/exchange?sslmode=disable")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st, err := store.NewPostgresStore(ctx, connString)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create postgres store")
	}
	defer st.Close()

	settler := consumer.NewSettler(st)
	handler := consumer.NewGroupHandler(settler)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	// No transactional producer exists yet, so nothing on the fills topic
	// is ever part of an aborted transaction today. Setting this now costs
	// nothing and avoids a silent gap the day the matcher gains real
	// Kafka transactions.
	cfg.Consumer.IsolationLevel = sarama.ReadCommitted

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
			if err := group.Consume(ctx, []string{"fills"}, handler); err != nil {
				log.Error().Err(err).Msg("consumer group session ended")
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Info().Str("group_id", groupID).Msg("settler consuming")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info().Msg("settler shutting down")
	cancel()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
