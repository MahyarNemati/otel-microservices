// Package gateway is the public edge: it accepts a checkout request and calls
// the orders service.
//
// It is the only service that starts traces, which makes it the place where
// head sampling would happen if this stack used head sampling. It does not —
// see deploy/collector/config.yaml — so the gateway samples everything and
// lets the collector decide what is worth keeping once it has seen the whole
// trace.
package gateway

import (
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/MahyarNemati/otel-microservices/internal/chaos"
	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/orders"
	"github.com/MahyarNemati/otel-microservices/internal/spanerr"
)

// Config configures a [Service].
type Config struct {
	// OrdersBaseURL is the orders service's base URL.
	OrdersBaseURL string
}

// Service is the gateway HTTP service.
type Service struct {
	cfg    Config
	client *http.Client
	chaos  *chaos.Injector
}

// NewService builds the gateway.
func NewService(cfg Config, client *http.Client, injector *chaos.Injector) *Service {
	return &Service{cfg: cfg, client: client, chaos: injector}
}

// CheckoutRequest is the public request body.
type CheckoutRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Customer string `json:"customer"`
}

// CheckoutResponse is the public response body. TraceID is echoed deliberately:
// it is how a human, the chaos script and the end-to-end test all find the
// trace for a request they just made.
type CheckoutResponse struct {
	OrderID   string `json:"order_id"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	Remaining int    `json:"remaining"`
	MessageID string `json:"message_id"`
	TraceID   string `json:"trace_id"`
}

// Routes returns the gateway's instrumented routes.
func (s *Service) Routes() []httpx.Route {
	return []httpx.Route{
		{Pattern: "POST /checkout", Name: "POST /checkout", Handler: s.handleCheckout},
		{Pattern: "GET /orders/{id}", Name: "GET /orders/{id}", Handler: s.handleOrder},
	}
}

func (s *Service) handleCheckout(w http.ResponseWriter, r *http.Request) {
	var req CheckoutRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if req.SKU == "" {
		httpx.WriteError(w, r, spanerr.NewHTTPError(http.StatusBadRequest, "missing_sku",
			fmt.Errorf("sku is required")))
		return
	}
	if req.Quantity <= 0 {
		req.Quantity = 1
	}

	httpx.SpanAttr(r,
		attribute.String("order.sku", req.SKU),
		attribute.Int("order.quantity", req.Quantity),
		attribute.String("enduser.id", req.Customer),
	)

	if err := s.chaos.Apply(r.Context()); err != nil {
		httpx.WriteError(w, r, spanerr.NewHTTPError(http.StatusServiceUnavailable, "chaos_fault", err))
		return
	}

	var downstream orders.CheckoutResponse
	if err := httpx.PostJSON(r, s.client, s.cfg.OrdersBaseURL+"/orders", orders.CheckoutRequest{
		SKU: req.SKU, Quantity: req.Quantity, Customer: req.Customer,
	}, &downstream); err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	httpx.WriteJSON(w, http.StatusAccepted, CheckoutResponse{
		OrderID:   downstream.OrderID,
		SKU:       downstream.SKU,
		Quantity:  downstream.Quantity,
		Remaining: downstream.Remaining,
		MessageID: downstream.MessageID,
		TraceID:   trace.SpanContextFromContext(r.Context()).TraceID().String(),
	})
}

func (s *Service) handleOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	httpx.SpanAttr(r, attribute.String("order.id", id))

	var order orders.Order
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, s.cfg.OrdersBaseURL+"/orders/"+id, nil)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if err := doJSON(s.client, req, &order); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, order)
}
