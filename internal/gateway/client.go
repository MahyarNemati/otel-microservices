package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/MahyarNemati/otel-microservices/internal/httpx"
	"github.com/MahyarNemati/otel-microservices/internal/spanerr"
)

// doJSON performs req and decodes a JSON response into out, mapping a non-2xx
// into an [spanerr.HTTPError] that carries the upstream status.
func doJSON(client *http.Client, req *http.Request, out any) error {
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("gateway: call %s: %w", req.URL, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		var upstream httpx.ErrorBody
		_ = json.NewDecoder(io.LimitReader(resp.Body, httpx.MaxBodyBytes)).Decode(&upstream)
		msg := upstream.Error
		if msg == "" {
			msg = resp.Status
		}
		code := upstream.Code
		if code == "" {
			code = "upstream_error"
		}
		return spanerr.NewHTTPError(resp.StatusCode, code, fmt.Errorf("%s: %s", req.URL, msg))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, httpx.MaxBodyBytes)).Decode(out); err != nil {
		return fmt.Errorf("gateway: decode response: %w", err)
	}
	return nil
}
