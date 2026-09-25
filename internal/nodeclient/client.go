// Package nodeclient is the coordinator's HTTP client for a storage node.
// It implements storage.StorageNode.
package nodeclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"vault/internal/integrity"
	"vault/internal/storage"
)

// StatusError is returned when a node answers with an unexpected HTTP status.
type StatusError struct {
	NodeID string
	Status int
	Code   string
	Msg    string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("node %s: HTTP %d %s: %s", e.NodeID, e.Status, e.Code, e.Msg)
}

// NewTransport returns an HTTP transport with bounded connection behaviour,
// suitable for sharing across all node clients.
func NewTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

// Client talks to one storage node.
type Client struct {
	id   string
	base *url.URL
	http *http.Client
}

var (
	_ storage.StorageNode = (*Client)(nil)
	_ storage.ChaosNode   = (*Client)(nil)
)

// New returns a client for the node id reachable at baseURL
// (e.g. "http://127.0.0.1:9001"). hc may be shared between clients.
func New(id, baseURL string, hc *http.Client) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("node %s: bad address %q: %w", id, baseURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("node %s: address %q must be an absolute http(s) URL", id, baseURL)
	}
	return &Client{id: id, base: u, http: hc}, nil
}

func (c *Client) ID() string      { return c.id }
func (c *Client) Address() string { return c.base.String() }

func (c *Client) url(parts ...string) string {
	u := *c.base
	escaped := make([]string, len(parts))
	for i, p := range parts {
		escaped[i] = url.PathEscape(p)
	}
	u.Path = u.Path + "/" + strings.Join(escaped, "/")
	return u.String()
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		// Context cancellation is the caller's decision, not a node fault.
		if ctxErr := req.Context().Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("%w: node %s: %v", storage.ErrUnavailable, c.id, err)
	}
	return resp, nil
}

// statusError drains and decodes an error response.
func (c *Client) statusError(resp *http.Response) error {
	defer resp.Body.Close()
	var body storage.ErrorBody
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if json.Unmarshal(b, &body) != nil {
		body.Error.Message = strings.TrimSpace(string(b))
	}
	se := &StatusError{NodeID: c.id, Status: resp.StatusCode, Code: body.Error.Code, Msg: body.Error.Message}
	switch resp.StatusCode {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %w", storage.ErrNotFound, se)
	case http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: %w", storage.ErrRejected, se)
	case http.StatusConflict:
		if se.Code == "stale_version" {
			return fmt.Errorf("%w: %w", storage.ErrStaleVersion, se)
		}
	case http.StatusForbidden:
		if se.Code == "chaos_disabled" {
			return fmt.Errorf("%w: %w", storage.ErrChaosDisabled, se)
		}
	}
	return se
}

func versionStr(v uint64) string { return strconv.FormatUint(v, 10) }

// PutReplica uploads body, declaring its expected digest to the node.
func (c *Client) PutReplica(ctx context.Context, objectID string, version uint64, want integrity.Digest, body io.Reader) (storage.ReplicaInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url("replicas", objectID, versionStr(version)), body)
	if err != nil {
		return storage.ReplicaInfo{}, err
	}
	req.ContentLength = want.Size
	if want.Size == 0 {
		req.Body = http.NoBody
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(storage.HeaderSHA256, want.SHA256)
	resp, err := c.do(req)
	if err != nil {
		return storage.ReplicaInfo{}, err
	}
	if resp.StatusCode != http.StatusCreated {
		return storage.ReplicaInfo{}, c.statusError(resp)
	}
	defer resp.Body.Close()
	var info storage.ReplicaInfo
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&info); err != nil {
		return storage.ReplicaInfo{}, fmt.Errorf("node %s: decode put response: %w", c.id, err)
	}
	return info, nil
}

// GetReplica streams the replica's bytes as stored by the node.
func (c *Client) GetReplica(ctx context.Context, objectID string, version uint64) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("replicas", objectID, versionStr(version)), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, c.statusError(resp)
	}
	return resp.Body, nil
}

func (c *Client) deleteReq(ctx context.Context, u string, okStatus int) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != okStatus {
		return c.statusError(resp)
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	return nil
}

// DeleteReplica removes one version from the node.
func (c *Client) DeleteReplica(ctx context.Context, objectID string, version uint64) error {
	return c.deleteReq(ctx, c.url("replicas", objectID, versionStr(version)), http.StatusNoContent)
}

// DeleteObject removes every version of objectID from the node.
func (c *Client) DeleteObject(ctx context.Context, objectID string) error {
	return c.deleteReq(ctx, c.url("replicas", objectID), http.StatusOK)
}

func (c *Client) getJSON(ctx context.Context, u string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return c.statusError(resp)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(v); err != nil {
		return fmt.Errorf("node %s: decode response: %w", c.id, err)
	}
	return nil
}

// Health fetches the node's self-reported health and capacity. A response
// from a different node ID than expected is treated as an error: it means
// the address is misconfigured, and trusting it would be unsafe.
func (c *Client) Health(ctx context.Context) (storage.NodeHealth, error) {
	var h storage.NodeHealth
	if err := c.getJSON(ctx, c.url("health"), &h); err != nil {
		return h, err
	}
	if h.NodeID != c.id {
		return h, fmt.Errorf("node at %s identifies as %q, expected %q", c.base, h.NodeID, c.id)
	}
	return h, nil
}

// VerifyReplica asks the node to re-hash a replica from its own disk.
func (c *Client) VerifyReplica(ctx context.Context, objectID string, version uint64) (storage.VerifyResult, error) {
	var res storage.VerifyResult
	err := c.getJSON(ctx, c.url("replicas", objectID, versionStr(version), "verify"), &res)
	return res, err
}

func (c *Client) post(ctx context.Context, u string, okStatus int, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != okStatus {
		return c.statusError(resp)
	}
	defer resp.Body.Close()
	if v == nil {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(v); err != nil {
		return fmt.Errorf("node %s: decode response: %w", c.id, err)
	}
	return nil
}

// Terminate asks the node process to exit (chaos endpoint).
func (c *Client) Terminate(ctx context.Context) error {
	return c.post(ctx, c.url("admin", "terminate"), http.StatusAccepted, nil)
}

// CorruptReplica asks the node to silently damage a stored replica on its
// own disk (chaos endpoint).
func (c *Client) CorruptReplica(ctx context.Context, objectID string, version uint64) (storage.CorruptResult, error) {
	var res storage.CorruptResult
	err := c.post(ctx, c.url("admin", "corrupt", objectID, versionStr(version)), http.StatusOK, &res)
	return res, err
}

// ListReplicas returns every version of objectID stored on the node.
func (c *Client) ListReplicas(ctx context.Context, objectID string) ([]storage.ReplicaInfo, error) {
	var body struct {
		Replicas []storage.ReplicaInfo `json:"replicas"`
	}
	err := c.getJSON(ctx, c.url("replicas", objectID), &body)
	return body.Replicas, err
}

// Inventory returns every replica stored on the node.
func (c *Client) Inventory(ctx context.Context) ([]storage.ReplicaInfo, error) {
	var body struct {
		Replicas []storage.ReplicaInfo `json:"replicas"`
	}
	u := *c.base
	u.Path += "/replicas"
	err := c.getJSON(ctx, u.String(), &body)
	return body.Replicas, err
}
