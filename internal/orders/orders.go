// Package orders is the middle service: it validates a checkout, reserves stock
// over HTTP, persists the order, and publishes a fulfilment job onto the queue
// that the Node worker consumes.
//
// It is the service where the trace branches, and therefore the one whose
// waterfall is worth looking at: a synchronous child (the reservation), a
// datastore write, and a producer span whose child appears in a different
// process, in a different language, some time later.
package orders

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/MahyarNemati/otel-microservices/internal/chaos"
	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/inventory"
	"github.com/MahyarNemati/otel-microservices/internal/metrics"
	"github.com/MahyarNemati/otel-microservices/internal/queue"
	"github.com/MahyarNemati/otel-microservices/internal/spanerr"
)

// TopicFulfil is the queue topic the worker consumes.
const TopicFulfil = "orders.fulfil"

// Order is a persisted order.
type Order struct {
	ID       string    `json:"id"`
	SKU      string    `json:"sku"`
	Quantity int       `json:"quantity"`
	Customer string    `json:"customer"`
	Status   string    `json:"status"`
	Created  time.Time `json:"created_at"`
}

// Store persists orders. In-memory, for the reason given in the inventory
// package: the subject here is the trace, not the storage engine.
type Store struct {
	mu     sync.Mutex
	orders map[string]Order
	tracer trace.Tracer
}

// NewStore builds an empty store.
func NewStore(tracer trace.Tracer) *Store {
	return &Store{orders: make(map[string]Order), tracer: tracer}
}

// Save writes an order inside its own span, so the persistence step shows up as
// a segment of the waterfall rather than as unexplained handler time.
func (s *Store) Save(ctx context.Context, order Order) error {
	_, span := s.tracer.Start(ctx, "orders.insert",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "orders.memory"),
			attribute.String("db.operation.name", "insert"),
			attribute.String("db.collection.name", "orders"),
			attribute.String("order.id", order.ID),
		),
	)
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.orders[order.ID]; exists {
		return spanerr.RecordClient(span, spanerr.NewHTTPError(http.StatusConflict, "duplicate_order",
			fmt.Errorf("order %s already exists", order.ID)))
	}
	s.orders[order.ID] = order
	return nil
}

// Get returns an order by id.
func (s *Store) Get(id string) (Order, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.orders[id]
	return o, ok
}

// Count reports how many orders are stored.
func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.orders)
}

// FulfilJob is the queue payload. EnqueuedAt lets the worker report real queue
// wait time rather than guessing from span timestamps, which is the only way to
// get that number right when the two processes have different clocks.
type FulfilJob struct {
	OrderID    string    `json:"order_id"`
	SKU        string    `json:"sku"`
	Quantity   int       `json:"quantity"`
	Customer   string    `json:"customer"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// Config configures a [Service].
type Config struct {
	// InventoryBaseURL is the inventory service's base URL.
	InventoryBaseURL string
	// NewOrderID generates order ids. Overridden in tests for determinism.
	NewOrderID func() string
}

// Service is the orders HTTP service.
type Service struct {
	cfg       Config
	store     *Store
	publisher Publisher
	inventory *http.Client
	chaos     *chaos.Injector
	tracer    trace.Tracer
	metrics   *metrics.Registry
}

// Publisher is the subset of the queue publisher this service needs, so tests
// can supply a double without a transport.
type Publisher interface {
	Publish(ctx context.Context, topic string, body any) (queue.Message, error)
}

// NewService builds the service.
func NewService(cfg Config, store *Store, publisher Publisher, client *http.Client, injector *chaos.Injector, tracer trace.Tracer, reg *metrics.Registry) *Service {
	if cfg.NewOrderID == nil {
		cfg.NewOrderID = newOrderID
	}
	return &Service{
		cfg:       cfg,
		store:     store,
		publisher: publisher,
		inventory: client,
		chaos:     injector,
		tracer:    tracer,
		metrics:   reg,
	}
}

// CheckoutRequest is the request body of POST /orders.
type CheckoutRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Customer string `json:"customer"`
}

// CheckoutResponse is the success body of POST /orders.
type CheckoutResponse struct {
	OrderID   string `json:"order_id"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	Remaining int    `json:"remaining"`
	MessageID string `json:"message_id"`
	TraceID   string `json:"trace_id"`
}

// Routes returns the service's instrumented routes.
func (s *Service) Routes() []httpx.Route {
	return []httpx.Route{
		{Pattern: "POST /orders", Name: "POST /orders", Handler: s.handleCheckout},
		{Pattern: "GET /orders/{id}", Name: "GET /orders/{id}", Handler: s.handleGet},
	}
}

func (s *Service) handleCheckout(w http.ResponseWriter, r *http.Request) {
	var req CheckoutRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if req.SKU == "" || req.Quantity <= 0 {
		httpx.WriteError(w, r, spanerr.NewHTTPError(http.StatusBadRequest, "invalid_checkout",
			fmt.Errorf("sku must be set and quantity must be positive, got %q and %d", req.SKU, req.Quantity)))
		return
	}

	order := Order{
		ID:       s.cfg.NewOrderID(),
		SKU:      req.SKU,
		Quantity: req.Quantity,
		Customer: req.Customer,
		Status:   "pending",
		Created:  time.Now().UTC(),
	}

	// Attributes on the server span, not on a child: these are the dimensions
	// someone filters a Tempo search by.
	httpx.SpanAttr(r,
		attribute.String("order.id", order.ID),
		attribute.String("order.sku", order.SKU),
		attribute.Int("order.quantity", order.Quantity),
	)

	if err := s.chaos.Apply(r.Context()); err != nil {
		httpx.WriteError(w, r, spanerr.NewHTTPError(http.StatusServiceUnavailable, "chaos_fault", err))
		return
	}

	// Synchronous hop. The client span is created by otelhttp's transport and
	// the traceparent header goes with it; the inventory service's server span
	// becomes its child.
	var reserved inventory.ReserveResponse
	reserveURL := s.cfg.InventoryBaseURL + "/reserve"
	if err := httpx.PostJSON(r, s.inventory, reserveURL, inventory.ReserveRequest{
		SKU: order.SKU, Quantity: order.Quantity, OrderID: order.ID,
	}, &reserved); err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	if err := s.store.Save(r.Context(), order); err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	// Asynchronous hop. See internal/queue for why this is the interesting one.
	msg, err := s.publisher.Publish(r.Context(), TopicFulfil, FulfilJob{
		OrderID:    order.ID,
		SKU:        order.SKU,
		Quantity:   order.Quantity,
		Customer:   order.Customer,
		EnqueuedAt: time.Now().UTC(),
	})
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	traceID := trace.SpanContextFromContext(r.Context()).TraceID().String()
	httpx.WriteJSON(w, http.StatusAccepted, CheckoutResponse{
		OrderID:   order.ID,
		SKU:       order.SKU,
		Quantity:  order.Quantity,
		Remaining: reserved.Remaining,
		MessageID: msg.ID,
		TraceID:   traceID,
	})
}

func (s *Service) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	httpx.SpanAttr(r, attribute.String("order.id", id))

	order, ok := s.store.Get(id)
	if !ok {
		httpx.WriteError(w, r, spanerr.NewHTTPError(http.StatusNotFound, "order_not_found",
			fmt.Errorf("order %s not found", id)))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, order)
}

func newOrderID() string {
	return fmt.Sprintf("ord-%d", time.Now().UnixNano())
}
