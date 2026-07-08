CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE accounts (
  user_id       UUID PRIMARY KEY,
  cash_cents    BIGINT NOT NULL DEFAULT 0,
  shares_qqq    BIGINT NOT NULL DEFAULT 0,
  version       INT NOT NULL DEFAULT 1,
  updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE transactions (
  trade_id UUID PRIMARY KEY,
  buy_order_id UUID NOT NULL,
  sell_order_id UUID NOT NULL,
  buyer_id UUID NOT NULL,
  seller_id UUID NOT NULL,
  quantity BIGINT NOT NULL,
  price_cents BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transactions_buyer ON transactions(buyer_id);
CREATE INDEX idx_transactions_seller ON transactions(seller_id);
CREATE INDEX idx_transactions_buy_order ON transactions(buy_order_id);
CREATE INDEX idx_transactions_sell_order ON transactions(sell_order_id);
CREATE INDEX idx_transactions_created ON transactions(created_at);

CREATE TABLE pending_orders (
  order_id           UUID PRIMARY KEY,
  user_id            UUID NOT NULL,
  side               VARCHAR(4) NOT NULL,
  quantity_requested BIGINT NOT NULL,
  quantity_filled    BIGINT NOT NULL DEFAULT 0,
  price_cents        BIGINT NOT NULL,
  created_at         TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pending_user       ON pending_orders(user_id);
CREATE INDEX idx_pending_side_price ON pending_orders(side, price_cents);
CREATE INDEX idx_pending_created    ON pending_orders(created_at);

CREATE TABLE dlq_messages (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id     UUID,
  error_reason VARCHAR(255) NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dlq_order ON dlq_messages(order_id);
