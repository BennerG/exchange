package store

import (
	"context"
	"fmt"
)

func (p *PostgresStore) CommitMatch(ctx context.Context, updates []PendingUpdate, deletes []string, insert *PendingOrder) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, u := range updates {
		if _, err := tx.Exec(ctx, `
			UPDATE pending_orders
			SET quantity_filled = quantity_requested - $2, updated_at = NOW()
			WHERE order_id = $1::uuid
		`, u.OrderID, u.Remaining); err != nil {
			return fmt.Errorf("update pending order %s: %w", u.OrderID, err)
		}
	}

	for _, id := range deletes {
		if _, err := tx.Exec(ctx, `DELETE FROM pending_orders WHERE order_id = $1::uuid`, id); err != nil {
			return fmt.Errorf("delete pending order %s: %w", id, err)
		}
	}

	if insert != nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO pending_orders (order_id, user_id, side, quantity_requested, quantity_filled, price_cents, created_at)
			VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7)
		`, insert.OrderID, insert.UserID, insert.Side, insert.QuantityRequested, insert.QuantityFilled, insert.PriceCents, insert.CreatedAt); err != nil {
			return fmt.Errorf("insert pending order %s: %w", insert.OrderID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (p *PostgresStore) LoadAll(ctx context.Context) ([]PendingOrder, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT order_id, user_id, side, quantity_requested, quantity_filled, price_cents, created_at
		FROM pending_orders
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query pending orders: %w", err)
	}
	defer rows.Close()

	var result []PendingOrder
	for rows.Next() {
		var po PendingOrder
		if err := rows.Scan(&po.OrderID, &po.UserID, &po.Side, &po.QuantityRequested, &po.QuantityFilled, &po.PriceCents, &po.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan pending order: %w", err)
		}
		result = append(result, po)
	}
	return result, rows.Err()
}
