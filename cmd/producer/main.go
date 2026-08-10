package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/BennerG/exchange/internal/metrics"
	"github.com/BennerG/exchange/internal/producer"
)

func main() {
	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	go func() {
		addr := envOr("METRICS_ADDR", ":2112")
		if err := metrics.Serve(addr); err != nil {
			log.Error().Err(err).Msg("metrics server error")
		}
	}()

	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	addr := envOr("ADDR", ":8080")

	pub, err := producer.NewKafkaPublisher(brokers)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kafka publisher")
	}
	defer pub.Close()

	srv := &http.Server{
		Addr:    addr,
		Handler: producer.NewHandler(pub),
	}

	go func() {
		log.Info().Str("addr", addr).Msg("producer listening")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("server error")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("shutdown error")
	}
	log.Info().Msg("producer stopped")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
