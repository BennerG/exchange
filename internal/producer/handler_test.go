package producer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/producer"
)

// stubPublisher captures the last event published so tests can assert on it.
type stubPublisher struct {
	published *pb.Event
	err       error
}

func (s *stubPublisher) Publish(_ context.Context, event *pb.Event) error {
	s.published = event
	return s.err
}

func post(t *testing.T, h http.Handler, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// TestSubmitBuyOrder verifies that a well-formed buy order is accepted, assigned
// a UUID order ID, and published as an OrderSubmitted event.
func TestSubmitBuyOrder(t *testing.T) {
	pub := &stubPublisher{}
	h := producer.NewHandler(pub)

	rr := post(t, h, map[string]any{
		"user_id":  "user-abc",
		"quantity": 100,
		"price_per_share": map[string]any{
			"amount_cents": 47500,
			"currency":     "USD",
		},
		"side": "BUY",
	})

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: want 202, got %d — body: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["order_id"] == "" {
		t.Error("expected non-empty order_id in response")
	}

	if pub.published == nil {
		t.Fatal("expected event to be published")
	}
	submitted := pub.published.GetOrderSubmitted()
	if submitted == nil {
		t.Fatal("expected OrderSubmitted payload")
	}
	if submitted.UserId != "user-abc" {
		t.Errorf("user_id: want user-abc, got %s", submitted.UserId)
	}
	if submitted.Quantity != 100 {
		t.Errorf("quantity: want 100, got %d", submitted.Quantity)
	}
	if submitted.PricePerShare.AmountCents != 47500 {
		t.Errorf("price_cents: want 47500, got %d", submitted.PricePerShare.AmountCents)
	}
	if submitted.Side != pb.OrderSide_BUY {
		t.Errorf("side: want BUY, got %v", submitted.Side)
	}
}

// TestSubmitSellOrder confirms the handler handles the SELL side correctly.
func TestSubmitSellOrder(t *testing.T) {
	pub := &stubPublisher{}
	h := producer.NewHandler(pub)

	rr := post(t, h, map[string]any{
		"user_id":  "user-xyz",
		"quantity": 50,
		"price_per_share": map[string]any{
			"amount_cents": 47600,
			"currency":     "USD",
		},
		"side": "SELL",
	})

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: want 202, got %d", rr.Code)
	}
	submitted := pub.published.GetOrderSubmitted()
	if submitted.Side != pb.OrderSide_SELL {
		t.Errorf("side: want SELL, got %v", submitted.Side)
	}
}

// TestMissingUserID verifies that a request without user_id is rejected with 400.
func TestMissingUserID(t *testing.T) {
	pub := &stubPublisher{}
	h := producer.NewHandler(pub)

	rr := post(t, h, map[string]any{
		"quantity": 100,
		"price_per_share": map[string]any{
			"amount_cents": 47500,
			"currency":     "USD",
		},
		"side": "BUY",
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rr.Code)
	}
	if pub.published != nil {
		t.Error("should not publish when validation fails")
	}
}

// TestZeroQuantity verifies that quantity=0 is rejected; you cannot submit an
// order for zero shares.
func TestZeroQuantity(t *testing.T) {
	pub := &stubPublisher{}
	h := producer.NewHandler(pub)

	rr := post(t, h, map[string]any{
		"user_id":  "user-abc",
		"quantity": 0,
		"price_per_share": map[string]any{
			"amount_cents": 47500,
			"currency":     "USD",
		},
		"side": "BUY",
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rr.Code)
	}
}

// TestNegativeQuantity verifies that negative share counts are rejected.
func TestNegativeQuantity(t *testing.T) {
	pub := &stubPublisher{}
	h := producer.NewHandler(pub)

	rr := post(t, h, map[string]any{
		"user_id":  "user-abc",
		"quantity": -10,
		"price_per_share": map[string]any{
			"amount_cents": 47500,
			"currency":     "USD",
		},
		"side": "BUY",
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rr.Code)
	}
}

// TestZeroPriceCents verifies that a price of 0 cents is rejected.
func TestZeroPriceCents(t *testing.T) {
	pub := &stubPublisher{}
	h := producer.NewHandler(pub)

	rr := post(t, h, map[string]any{
		"user_id":  "user-abc",
		"quantity": 100,
		"price_per_share": map[string]any{
			"amount_cents": 0,
			"currency":     "USD",
		},
		"side": "BUY",
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rr.Code)
	}
}

// TestInvalidSide verifies that an unrecognized side string is rejected.
func TestInvalidSide(t *testing.T) {
	pub := &stubPublisher{}
	h := producer.NewHandler(pub)

	rr := post(t, h, map[string]any{
		"user_id":  "user-abc",
		"quantity": 100,
		"price_per_share": map[string]any{
			"amount_cents": 47500,
			"currency":     "USD",
		},
		"side": "HOLD",
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rr.Code)
	}
}

// TestOrderIDIsStableInResponse verifies that the order_id returned to the
// caller matches the order_id embedded in the published event, so the client
// can poll for status using the same ID.
func TestOrderIDIsStableInResponse(t *testing.T) {
	pub := &stubPublisher{}
	h := producer.NewHandler(pub)

	rr := post(t, h, map[string]any{
		"user_id":  "user-abc",
		"quantity": 100,
		"price_per_share": map[string]any{
			"amount_cents": 47500,
			"currency":     "USD",
		},
		"side": "BUY",
	})

	var resp map[string]string
	json.NewDecoder(rr.Body).Decode(&resp)

	eventOrderID := pub.published.GetOrderSubmitted().OrderId
	if resp["order_id"] != eventOrderID {
		t.Errorf("response order_id %q does not match published event order_id %q",
			resp["order_id"], eventOrderID)
	}
}
