package workflowcheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/observability"
)

type client struct {
	baseURL string
	http    *http.Client
}

type response struct {
	status      int
	body        []byte
	operationID string
}

func newClient(baseURL string) *client {
	return &client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// call issues a request with a fresh operation id on every invocation, so
// concurrent requests can never share one correlation id.
func (c *client) call(
	method string,
	path string,
	payload any,
	expectedVersion *int64,
) (response, error) {
	return c.callWithOperation(method, path, payload, expectedVersion, "", 1)
}

// callWithOperation issues a request carrying an explicit stable operation id.
// A terminal retry passes the same operationID with an incremented attempt.
func (c *client) callWithOperation(
	method string,
	path string,
	payload any,
	expectedVersion *int64,
	operationID string,
	attempt int,
) (response, error) {
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
	if operationID == "" {
		generated, err := observability.NewOperationID()
		if err != nil {
			return response{}, err
		}
		operationID = generated
	}
	if attempt < 1 {
		attempt = 1
	}
	request.Header.Set(observability.OperationHeader, operationID)
	request.Header.Set(observability.AttemptHeader, fmt.Sprintf("%d", attempt))
	raw, err := c.http.Do(request)
	if err != nil {
		return response{}, fmt.Errorf("perform request op=%s: %w", operationID, err)
	}
	defer raw.Body.Close()
	content, err := io.ReadAll(io.LimitReader(raw.Body, 2<<20))
	if err != nil {
		return response{}, fmt.Errorf("read response op=%s: %w", operationID, err)
	}
	return response{
		status:      raw.StatusCode,
		body:        content,
		operationID: raw.Header.Get(observability.OperationHeader),
	}, nil
}

// callWithRawHeaders issues a request with fully caller-controlled headers,
// used to exercise malformed correlation headers.
func (c *client) callWithRawHeaders(
	method string,
	path string,
	payload any,
	headers map[string]string,
) (response, error) {
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
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	raw, err := c.http.Do(request)
	if err != nil {
		return response{}, fmt.Errorf("perform request: %w", err)
	}
	defer raw.Body.Close()
	content, err := io.ReadAll(io.LimitReader(raw.Body, 2<<20))
	if err != nil {
		return response{}, fmt.Errorf("read response: %w", err)
	}
	return response{
		status:      raw.StatusCode,
		body:        content,
		operationID: raw.Header.Get(observability.OperationHeader),
	}, nil
}

// retryWithSameOperation simulates a terminal retry after a failure: the
// operator resends the exact action reusing the stable operation id.
func (c *client) retryWithSameOperation(
	first response,
	method string,
	path string,
	payload any,
	expectedVersion *int64,
) (response, error) {
	if !observability.ValidOperationID(first.operationID) {
		return response{}, fmt.Errorf("first attempt returned no valid operation id")
	}
	return c.callWithOperation(method, path, payload, expectedVersion,
		first.operationID, 2)
}

func (r response) decode(target any) error {
	if err := json.Unmarshal(r.body, target); err != nil {
		return fmt.Errorf("decode response op=%s %q: %w",
			r.operationID, string(r.body), err)
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

func requireStatus(actual response, expected int, operation string) error {
	if actual.status != expected {
		return fmt.Errorf("%s [op=%s]: expected status %d, got %d: %s",
			operation, actual.operationID, expected, actual.status,
			strings.TrimSpace(string(actual.body)))
	}
	return nil
}

func requireCode(actual response, expectedStatus int, expectedCode, operation string) error {
	if err := requireStatus(actual, expectedStatus, operation); err != nil {
		return err
	}
	if actual.errorCode() != expectedCode {
		return fmt.Errorf("%s [op=%s]: expected error %q, got %q",
			operation, actual.operationID, expectedCode, actual.errorCode())
	}
	return nil
}
