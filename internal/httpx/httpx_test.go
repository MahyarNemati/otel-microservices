package httpx_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/obstest"
)

func newServer(t *testing.T, rec *obstest.Recorder) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(httpx.NewServerHandler(httpx.ServerOptions{
		ServiceName: "edge",
		Routes: []httpx.Route{{
			Pattern: "POST /thing/{id}",
			Name:    "POST /thing/{id}",
			Handler: func(w http.ResponseWriter, r *http.Request) {
				httpx.WriteJSON(w, http.StatusOK, map[string]string{"id": r.PathValue("id")})
			},
		}},
		Plain:          map[string]http.Handler{"/healthz": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })},
		TracerProvider: rec.Provider(),
		Propagators:    rec.Propagator,
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestSamplingPriorityHeaderSetsSpanAttribute covers the escape hatch from tail
// sampling. Without it, a fast successful trace is kept with 20% probability,
// so "run this one request and watch it" is a lottery and the end-to-end check
// cannot assert on a specific trace at all.
func TestSamplingPriorityHeaderSetsSpanAttribute(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	srv := newServer(t, rec)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/thing/42", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	// Case-insensitive on purpose: a header value is not a protocol constant.
	req.Header.Set(httpx.SamplingPriorityHeader, "HIGH")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	span := rec.Span(t, "POST /thing/{id}")
	// The collector's `explicitly-requested` policy matches on exactly this.
	if got := obstest.RequireAttr(t, span, httpx.AttrSamplingPriorityHigh); got != "true" {
		t.Errorf("%s = %q, want true", httpx.AttrSamplingPriorityHigh, got)
	}
}

func TestSamplingPriorityAbsentByDefault(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	srv := newServer(t, rec)

	resp, err := srv.Client().Post(srv.URL+"/thing/42", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	span := rec.Span(t, "POST /thing/{id}")
	if _, ok := obstest.Attr(span, httpx.AttrSamplingPriorityHigh); ok {
		t.Errorf("%s is set on an ordinary request; every trace would then be kept", httpx.AttrSamplingPriorityHigh)
	}
	for _, other := range []string{"low", "", "yes", "1"} {
		rec.Reset()
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/thing/42", strings.NewReader("{}"))
		req.Header.Set(httpx.SamplingPriorityHeader, other)
		r, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("request with %q: %v", other, err)
		}
		_ = r.Body.Close()
		if _, ok := obstest.Attr(rec.Span(t, "POST /thing/{id}"), httpx.AttrSamplingPriorityHigh); ok {
			t.Errorf("header value %q set the priority attribute; only \"high\" should", other)
		}
	}
}

// TestRouteTemplateIsTheSpanName guards metric and span-name cardinality: the
// raw path would put ids into both.
func TestRouteTemplateIsTheSpanName(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	srv := newServer(t, rec)

	for _, id := range []string{"1", "2", "3"} {
		resp, err := srv.Client().Post(srv.URL+"/thing/"+id, "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		_ = resp.Body.Close()
	}

	names := rec.Names()
	if len(names) != 3 {
		t.Fatalf("recorded %d spans, want 3: %v", len(names), names)
	}
	for _, n := range names {
		if n != "POST /thing/{id}" {
			t.Errorf("span name %q is not the route template; ids in span names make aggregation useless", n)
		}
	}
	if got := obstest.RequireAttr(t, rec.Spans()[0], "http.route"); got != "POST /thing/{id}" {
		t.Errorf("http.route = %q", got)
	}
}

// TestPlainRoutesAreNotTraced covers the probe-noise decision: a healthcheck
// every two seconds would otherwise dominate Tempo.
func TestPlainRoutesAreNotTraced(t *testing.T) {
	t.Parallel()
	rec := obstest.New(t, obstest.Options{})
	srv := newServer(t, rec)

	resp, err := srv.Client().Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}
	if n := len(rec.Spans()); n != 0 {
		t.Errorf("/healthz produced %d spans, want 0: %v", n, rec.Names())
	}
}
