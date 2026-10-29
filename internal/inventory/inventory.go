// Package inventory is the leaf service: it holds stock levels and is where the
// deliberate error path lives.
//
// Three failure shapes are reachable on purpose, because a trace backend is
// only interesting when the traces are not all green:
//
//   - A SKU that does not exist is a client fault: 404, exception event
//     recorded for debuggability, span status left Unset so the service's
//     error-rate SLO is not polluted by callers sending nonsense.
//   - A SKU prefixed OOS- is out of stock: 409, also a client fault.
//   - A SKU prefixed ERR- panics inside the reservation. The panic is
//     recovered, recorded with a stack trace, and returned as a 500 with the
//     span status set to Error. This is the one the tail sampler keeps.
package inventory

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/MahyarNemati/otel-microservices/internal/chaos"
	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/spanerr"
)

// Prefixes that select a failure mode. They are part of the service's contract
// rather than a hidden test hook, so the chaos script and the README can refer
// to them.
const (
	// PrefixOutOfStock makes a reservation fail with 409.
	PrefixOutOfStock = "OOS-"
	// PrefixPanic makes the reservation panic, exercising the recovery path.
	PrefixPanic = "ERR-"
	// PrefixWorkerFailure reserves normally and then fails in the Node worker,
	// on the far side of the queue. Inventory has to stock it: a checkout that
	// 404s here never reaches the worker at all, so the interesting failure --
	// an error span parented on a publish span in another process -- would
	// never happen.
	PrefixWorkerFailure = "FAIL-"
)

// Store holds stock levels. In-memory on purpose: a real database would add a
// migration story without adding anything to the tracing story, and the
// instrumented client span is simulated faithfully by [Store.span].
type Store struct {
	mu     sync.Mutex
	levels map[string]int
	tracer trace.Tracer
}

// NewStore seeds a store with a small catalogue.
func NewStore(tracer trace.Tracer) *Store {
	return &Store{
		tracer: tracer,
		levels: map[string]int{
			"SKU-TEA":                     120,
			"SKU-COFFEE":                  40,
			"SKU-COCOA":                   8,
			PrefixOutOfStock + "MATCHA":   0,
			PrefixPanic + "BROKEN":        5,
			PrefixWorkerFailure + "CRATE": 500,
		},
	}
}

// ErrNotFound is returned for an unknown SKU.
var ErrNotFound = errors.New("sku not found")

// ErrOutOfStock is returned when the requested quantity is unavailable.
var ErrOutOfStock = errors.New("insufficient stock")

// Reserve decrements stock for sku, inside its own span so the datastore access
// is visible in the waterfall rather than hidden inside the handler's duration.
func (s *Store) Reserve(ctx context.Context, sku string, qty int) (remaining int, err error) {
	ctx, span := s.tracer.Start(ctx, "inventory.reserve",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "inventory.memory"),
			attribute.String("db.operation.name", "reserve"),
			attribute.String("inventory.sku", sku),
			attribute.Int("inventory.quantity", qty),
		),
	)
	defer span.End()
	_ = ctx

	// Recovering the panic here rather than in the HTTP middleware is
	// deliberate: the span that panicked is the one that should carry the stack
	// trace, and a recover in the outer middleware would attribute it to the
	// server span with the real frames already unwound.
	defer func() {
		if rec := recover(); rec != nil {
			err = spanerr.RecordServer(span, fmt.Errorf("inventory: reserve panicked: %v", rec))
			remaining = 0
		}
	}()

	if strings.HasPrefix(sku, PrefixPanic) {
		panic("corrupt stock ledger for " + sku)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	level, ok := s.levels[sku]
	if !ok {
		return 0, spanerr.RecordClient(span, spanerr.NewHTTPError(
			http.StatusNotFound, "sku_not_found", fmt.Errorf("%w: %s", ErrNotFound, sku)))
	}
	if level < qty {
		span.SetAttributes(attribute.Int("inventory.available", level))
		return level, spanerr.RecordClient(span, spanerr.NewHTTPError(
			http.StatusConflict, "out_of_stock",
			fmt.Errorf("%w: %s has %d, wanted %d", ErrOutOfStock, sku, level, qty)))
	}

	s.levels[sku] = level - qty
	span.SetAttributes(attribute.Int("inventory.remaining", s.levels[sku]))
	return s.levels[sku], nil
}

// Level reports the current stock for sku.
func (s *Store) Level(sku string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	level, ok := s.levels[sku]
	return level, ok
}

// Service is the inventory HTTP service.
type Service struct {
	store  *Store
	chaos  *chaos.Injector
	tracer trace.Tracer
}

// NewService builds the service.
func NewService(store *Store, injector *chaos.Injector, tracer trace.Tracer) *Service {
	return &Service{store: store, chaos: injector, tracer: tracer}
}

// ReserveRequest is the request body of POST /reserve.
type ReserveRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	OrderID  string `json:"order_id"`
}

// ReserveResponse is the success body of POST /reserve.
type ReserveResponse struct {
	SKU       string `json:"sku"`
	Reserved  int    `json:"reserved"`
	Remaining int    `json:"remaining"`
	OrderID   string `json:"order_id"`
}

// Routes returns the service's instrumented routes.
func (s *Service) Routes() []httpx.Route {
	return []httpx.Route{
		{Pattern: "POST /reserve", Name: "POST /reserve", Handler: s.handleReserve},
		{Pattern: "POST /commit", Name: "POST /commit", Handler: s.handleCommit},
		{Pattern: "GET /stock/{sku}", Name: "GET /stock/{sku}", Handler: s.handleStock},
	}
}

func (s *Service) handleReserve(w http.ResponseWriter, r *http.Request) {
	var req ReserveRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if req.SKU == "" || req.Quantity <= 0 {
		httpx.WriteError(w, r, spanerr.NewHTTPError(http.StatusBadRequest, "invalid_reservation",
			fmt.Errorf("sku must be set and quantity must be positive, got %q and %d", req.SKU, req.Quantity)))
		return
	}

	httpx.SpanAttr(r,
		attribute.String("inventory.sku", req.SKU),
		attribute.Int("inventory.quantity", req.Quantity),
		attribute.String("order.id", req.OrderID),
	)

	if err := s.chaos.Apply(r.Context()); err != nil {
		httpx.WriteError(w, r, spanerr.NewHTTPError(http.StatusServiceUnavailable, "chaos_fault", err))
		return
	}

	remaining, err := s.store.Reserve(r.Context(), req.SKU, req.Quantity)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, ReserveResponse{
		SKU: req.SKU, Reserved: req.Quantity, Remaining: remaining, OrderID: req.OrderID,
	})
}

// CommitRequest is the request body of POST /commit, called by the Node worker.
// Its only purpose is to prove that W3C context propagates Go -> queue -> Node
// -> Go: this span must land in the trace the gateway started.
type CommitRequest struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
}

// CommitResponse is the success body of POST /commit.
type CommitResponse struct {
	OrderID   string `json:"order_id"`
	Committed bool   `json:"committed"`
}

func (s *Service) handleCommit(w http.ResponseWriter, r *http.Request) {
	var req CommitRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.SpanAttr(r,
		attribute.String("order.id", req.OrderID),
		attribute.String("inventory.sku", req.SKU),
		attribute.String("inventory.caller_runtime", "node"),
	)
	if err := s.chaos.Apply(r.Context()); err != nil {
		httpx.WriteError(w, r, spanerr.NewHTTPError(http.StatusServiceUnavailable, "chaos_fault", err))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, CommitResponse{OrderID: req.OrderID, Committed: true})
}

func (s *Service) handleStock(w http.ResponseWriter, r *http.Request) {
	sku := r.PathValue("sku")
	httpx.SpanAttr(r, attribute.String("inventory.sku", sku))

	level, ok := s.store.Level(sku)
	if !ok {
		httpx.WriteError(w, r, spanerr.NewHTTPError(http.StatusNotFound, "sku_not_found",
			fmt.Errorf("%w: %s", ErrNotFound, sku)))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"sku": sku, "available": level})
}
