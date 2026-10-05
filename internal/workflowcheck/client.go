package workflowcheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/operation"
)

type client struct {
	baseURL         string
	http            *http.Client
	operationPrefix string
	sequence        int
}

type response struct {
	status      int
	body        []byte
	operationID string
}

func newClient(baseURL, operationPrefix string) *client {
	return &client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http: &http.Client{
			Timeout: 5 * time.Second,
		},
		operationPrefix: operationPrefix,
	}
}

// call issues one request under a stable operation identifier derived from
// the check run prefix, so every request the check makes can be found in the
// server operation log. The server must echo the same identifier back.
func (c *client) call(
	method string,
	path string,
	payload any,
	expectedVersion *int64,
) (response, error) {
	c.sequence++
	operationID := fmt.Sprintf("%s-%04d", c.operationPrefix, c.sequence)
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return response{}, fmt.Errorf("encode request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, c.baseURL+path, body)
	if err != nil {
		return response{}, fmt.Errorf("create request: %w", err)
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if expectedVersion != nil {
		request.Header.Set("X-Expected-Version", fmt.Sprintf("%d", *expectedVersion))
	}
	request.Header.Set(operation.HeaderName, operationID)
	raw, err := c.http.Do(request)
	if err != nil {
		return response{}, fmt.Errorf("perform request (operation %s): %w", operationID, err)
	}
	defer raw.Body.Close()
	content, err := io.ReadAll(io.LimitReader(raw.Body, 2<<20))
	if err != nil {
		return response{}, fmt.Errorf("read response (operation %s): %w", operationID, err)
	}
	echoed := raw.Header.Get(operation.HeaderName)
	if echoed != operationID {
		return response{}, fmt.Errorf("operation %s: response carried operation %q",
			operationID, echoed)
	}
	return response{status: raw.StatusCode, body: content, operationID: echoed}, nil
}

func (r response) decode(target any) error {
	if err := json.Unmarshal(r.body, target); err != nil {
		return fmt.Errorf("decode response %q: %w", string(r.body), err)
	}
	return nil
}

func (r response) errorCode() string {
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(r.body, &body); err != nil {
		return ""
	}
	return body.Error.Code
}

func requireStatus(actual response, expected int, action string) error {
	if actual.status != expected {
		return fmt.Errorf("%s (operation %s): expected status %d, got %d: %s",
			action, actual.operationID, expected, actual.status,
			strings.TrimSpace(string(actual.body)))
	}
	return nil
}

func requireCode(actual response, expectedStatus int, expectedCode, action string) error {
	if err := requireStatus(actual, expectedStatus, action); err != nil {
		return err
	}
	if actual.errorCode() != expectedCode {
		return fmt.Errorf("%s (operation %s): expected error %q, got %q",
			action, actual.operationID, expectedCode, actual.errorCode())
	}
	return nil
}
