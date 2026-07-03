package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Publisher sends a serialized event to the orders topic.
type Publisher interface {
	Publish(ctx context.Context, event *pb.Event) error
}

type orderRequest struct {
	UserID        string `json:"user_id"`
	Quantity      int64  `json:"quantity"`
	PricePerShare struct {
		AmountCents int64  `json:"amount_cents"`
		Currency    string `json:"currency"`
	} `json:"price_per_share"`
	Side string `json:"side"`
}

type Handler struct {
	pub Publisher
}

func NewHandler(pub Publisher) http.Handler {
	h := &Handler{pub: pub}
	mux := http.NewServeMux()
	mux.HandleFunc("/orders", h.submitOrder)
	return mux
}

func (h *Handler) submitOrder(w http.ResponseWriter, r *http.Request) {
	var req orderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if err := validateOrder(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	side, err := parseSide(req.Side)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	orderID := uuid.New().String()
	event := &pb.Event{
		Payload: &pb.Event_OrderSubmitted{
			OrderSubmitted: &pb.OrderSubmitted{
				OrderId:  orderID,
				UserId:   req.UserID,
				Quantity: req.Quantity,
				PricePerShare: &pb.Money{
					AmountCents: req.PricePerShare.AmountCents,
					Currency:    req.PricePerShare.Currency,
				},
				Side:        side,
				SubmittedAt: timestamppb.Now(),
			},
		},
	}

	if err := h.pub.Publish(r.Context(), event); err != nil {
		http.Error(w, "failed to publish order", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"order_id": orderID})
}

func validateOrder(req orderRequest) error {
	switch {
	case req.UserID == "":
		return fmt.Errorf("user_id is required")
	case req.Quantity <= 0:
		return fmt.Errorf("quantity must be greater than 0")
	case req.PricePerShare.AmountCents <= 0:
		return fmt.Errorf("price_per_share.amount_cents must be greater than 0")
	}
	return nil
}

func parseSide(s string) (pb.OrderSide, error) {
	switch s {
	case "BUY":
		return pb.OrderSide_BUY, nil
	case "SELL":
		return pb.OrderSide_SELL, nil
	default:
		return 0, fmt.Errorf("side must be BUY or SELL, got %q", s)
	}
}
