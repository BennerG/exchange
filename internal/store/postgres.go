package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const uniqueViolationCode = "23505"

// PostgresStore settles trades into PostgreSQL. Exactly-once semantics are
// enforced by the database itself, through the primary key on trade_id,
// not by any check performed in application code.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, connString string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (p *PostgresStore) Close() {
	p.pool.Close()
}

func (p *PostgresStore) SettleFill(ctx context.Context, trade Trade) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO transactions (trade_id, buy_order_id, sell_order_id, buyer_id, seller_id, quantity, price_cents)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid, $6, $7)
	`, trade.TradeID, trade.BuyOrderID, trade.SellOrderID, trade.BuyerUserID, trade.SellerUserID, trade.Quantity, trade.PriceCents)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolationCode {
			return ErrAlreadySettled
		}
		return fmt.Errorf("insert transaction: %w", err)
	}

	amountCents := trade.Quantity * trade.PriceCents

	if err := upsertAccount(ctx, tx, trade.BuyerUserID, -amountCents, trade.Quantity); err != nil {
		return fmt.Errorf("update buyer account: %w", err)
	}
	if err := upsertAccount(ctx, tx, trade.SellerUserID, amountCents, -trade.Quantity); err != nil {
		return fmt.Errorf("update seller account: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func upsertAccount(ctx context.Context, tx pgx.Tx, userID string, cashDeltaCents, sharesDelta int64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO accounts (user_id, cash_cents, shares_qqq)
		VALUES ($1::uuid, $2, $3)
		ON CONFLICT (user_id) DO UPDATE
		SET cash_cents = accounts.cash_cents + $2,
		    shares_qqq = accounts.shares_qqq + $3,
		    version = accounts.version + 1,
		    updated_at = NOW()
	`, userID, cashDeltaCents, sharesDelta)
	return err
}

// Balance returns a snapshot of one account's holdings, for tests and for a
// future balance inquiry endpoint. Zero values mean the account has no
// transactions yet, not an error.
func (p *PostgresStore) Balance(ctx context.Context, userID string) (cashCents, sharesQQQ int64, err error) {
	err = p.pool.QueryRow(ctx, `
		SELECT cash_cents, shares_qqq FROM accounts WHERE user_id = $1::uuid
	`, userID).Scan(&cashCents, &sharesQQQ)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, nil
	}
	return cashCents, sharesQQQ, err
}
