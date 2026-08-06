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

const (
	testBuyerID  = "a1b2c3d4-1111-4a1a-9c1a-000000000001"
	testSellerID = "a1b2c3d4-1111-4a1a-9c1a-000000000002"
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
		"user_id":  testBuyerID,
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
	if submitted.GetUserId() != testBuyerID {
		t.Errorf("user_id: want %s, got %s", testBuyerID, submitted.GetUserId())
	}
	if submitted.GetQuantity() != 100 {
		t.Errorf("quantity: want 100, got %d", submitted.GetQuantity())
	}
	if submitted.GetPricePerShare().GetAmountCents() != 47500 {
		t.Errorf("price_cents: want 47500, got %d", submitted.GetPricePerShare().GetAmountCents())
	}
	if submitted.GetSide() != pb.OrderSide_BUY {
		t.Errorf("side: want BUY, got %v", submitted.GetSide())
	}
}

// TestSubmitSellOrder confirms the handler handles the SELL side correctly.
func TestSubmitSellOrder(t *testing.T) {
	pub := &stubPublisher{}
	h := producer.NewHandler(pub)

	rr := post(t, h, map[string]any{
		"user_id":  testSellerID,
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
	if submitted.GetSide() != pb.OrderSide_SELL {
		t.Errorf("side: want SELL, got %v", submitted.GetSide())
	}
}

// TestOrderIDIsStableInResponse verifies that the order_id returned to the
// caller matches the order_id embedded in the published event, so the client
// can poll for status using the same ID.
func TestOrderIDIsStableInResponse(t *testing.T) {
	pub := &stubPublisher{}
	h := producer.NewHandler(pub)

	rr := post(t, h, map[string]any{
		"user_id":  testBuyerID,
		"quantity": 100,
		"price_per_share": map[string]any{
			"amount_cents": 47500,
			"currency":     "USD",
		},
		"side": "BUY",
	})

	var resp map[string]string
	json.NewDecoder(rr.Body).Decode(&resp)

	eventOrderID := pub.published.GetOrderSubmitted().GetOrderId()
	if resp["order_id"] != eventOrderID {
		t.Errorf("response order_id %q does not match published event order_id %q",
			resp["order_id"], eventOrderID)
	}
}

// TestValidationRejectsInvalidOrders covers every input that submitOrder
// should reject with a 400 before ever calling Publish. Each case isolates
// exactly one invalid field, with every other field left valid, so a
// failure here can only be coming from the field the case names, not from
// an unrelated validation check firing first.
func TestValidationRejectsInvalidOrders(t *testing.T) {
	validPricePerShare := map[string]any{"amount_cents": 47500, "currency": "USD"}

	cases := []struct {
		name string
		body map[string]any
	}{
		{
			name: "missing user_id",
			body: map[string]any{
				"quantity": 100, "price_per_share": validPricePerShare, "side": "BUY",
			},
		},
		{
			name: "malformed user_id",
			body: map[string]any{
				"user_id": "not-a-uuid", "quantity": 100, "price_per_share": validPricePerShare, "side": "BUY",
			},
		},
		{
			name: "zero quantity",
			body: map[string]any{
				"user_id": testBuyerID, "quantity": 0, "price_per_share": validPricePerShare, "side": "BUY",
			},
		},
		{
			name: "negative quantity",
			body: map[string]any{
				"user_id": testBuyerID, "quantity": -10, "price_per_share": validPricePerShare, "side": "BUY",
			},
		},
		{
			name: "zero price",
			body: map[string]any{
				"user_id": testBuyerID, "quantity": 100,
				"price_per_share": map[string]any{"amount_cents": 0, "currency": "USD"}, "side": "BUY",
			},
		},
		{
			name: "invalid side",
			body: map[string]any{
				"user_id": testBuyerID, "quantity": 100, "price_per_share": validPricePerShare, "side": "HOLD",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := &stubPublisher{}
			h := producer.NewHandler(pub)

			rr := post(t, h, tc.body)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("status: want 400, got %d", rr.Code)
			}
			if pub.published != nil {
				t.Error("should not publish when validation fails")
			}
		})
	}
}
