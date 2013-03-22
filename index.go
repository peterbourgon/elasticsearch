package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
)

type BulkResponse struct {
	Took int `json:"took"` // ms

	Items []BulkItemResponse `json:"items"`
}

type BulkItemResponse IndexResponse

// Bulk responses are wrapped in an extra object whose only key is the
// operation performed (create, delete, or index). BulkItemResponse response is
// an alias for IndexResponse, but deals with this extra indirection.
func (r *BulkItemResponse) UnmarshalJSON(data []byte) error {
	var wrapper struct {
		Create json.RawMessage `json:"create"`
		Delete json.RawMessage `json:"delete"`
		Index  json.RawMessage `json:"index"`
	}

	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}

	var inner json.RawMessage

	switch {
	case wrapper.Create != nil:
		inner = wrapper.Create
	case wrapper.Index != nil:
		inner = wrapper.Index
	case wrapper.Delete != nil:
		inner = wrapper.Delete
	default:
		return fmt.Errorf("expected bulk response to be create, index, or delete")
	}

	if err := json.Unmarshal(inner, (*IndexResponse)(r)); err != nil {
		return err
	}

	return nil
}

type IndexResponse struct {
	Found   bool   `json:"found"`
	ID      string `json:"_id"`
	Index   string `json:"_index"`
	OK      bool   `json:"ok"`
	Type    string `json:"_type"`
	Version int    `json:"_version"`

	Error    string `json:"error,omitempty"`
	Status   int    `json:"status,omitempty"`
	TimedOut bool   `json:"timed_out,omitempty"`
}

type IndexParams struct {
	Index string `json:"_index"`
	Type  string `json:"_type"`
	Id    string `json:"_id"`

	Consistency string `json:"_consistency,omitempty"`
	Parent      string `json:"_parent,omitempty"`
	Percolate   string `json:"_percolate,omitempty"`
	Refresh     string `json:"_refresh,omitempty"`
	Replication string `json:"_replication,omitempty"`
	Routing     string `json:"_routing,omitempty"`
	TTL         string `json:"_ttl,omitempty"`
	Timestamp   string `json:"_timestamp,omitempty"`
	Version     string `json:"_version,omitempty"`
	VersionType string `json:"_version_type,omitempty"`
}

func (p IndexParams) Values() url.Values {
	return values(map[string]string{
		"consistency":  p.Consistency,
		"parent":       p.Parent,
		"percolate":    p.Percolate,
		"refresh":      p.Refresh,
		"replication":  p.Replication,
		"routing":      p.Routing,
		"ttl":          p.TTL,
		"timestamp":    p.Timestamp,
		"version":      p.Version,
		"version_type": p.VersionType,
	})
}

type IndexRequest struct {
	Params IndexParams
	Source interface{}
}

func (r IndexRequest) EncodeBulkHeader(enc *json.Encoder) error {
	return enc.Encode(map[string]IndexParams{
		"index": r.Params,
	})
}

func (r IndexRequest) EncodeSource(enc *json.Encoder) error {
	return enc.Encode(r.Source)
}

func (r IndexRequest) Request(uri *url.URL) (*http.Request, error) {
	uri.Path = path.Join("/", r.Params.Index, r.Params.Type, r.Params.Id)
	uri.RawQuery = r.Params.Values().Encode()

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)

	if err := r.EncodeSource(enc); err != nil {
		return nil, err
	}

	return http.NewRequest("PUT", uri.String(), buf)
}

type CreateRequest struct {
	Params IndexParams
	Source interface{}
}

func (r CreateRequest) EncodeBulkHeader(enc *json.Encoder) error {
	return enc.Encode(map[string]IndexParams{
		"create": r.Params,
	})
}

func (r CreateRequest) EncodeSource(enc *json.Encoder) error {
	return enc.Encode(r.Source)
}

func (r CreateRequest) Request(uri *url.URL) (*http.Request, error) {
	uri.Path = path.Join("/", r.Params.Index, r.Params.Type, r.Params.Id, "_create")
	uri.RawQuery = r.Params.Values().Encode()

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)

	if err := r.EncodeSource(enc); err != nil {
		return nil, err
	}

	return http.NewRequest("PUT", uri.String(), buf)
}

type DeleteRequest struct {
	Params IndexParams
}

func (r DeleteRequest) EncodeBulkHeader(enc *json.Encoder) error {
	return enc.Encode(map[string]IndexParams{
		"delete": r.Params,
	})
}

func (r DeleteRequest) EncodeSource(enc *json.Encoder) error {
	return nil
}

func (r DeleteRequest) Request(uri *url.URL) (*http.Request, error) {
	uri.Path = path.Join("/", r.Params.Index, r.Params.Type, r.Params.Id)
	uri.RawQuery = r.Params.Values().Encode()

	return http.NewRequest("DELETE", uri.String(), nil)
}

type UpdateRequest struct {
	Params IndexParams
	Source interface{}
}

func (r UpdateRequest) Request(uri *url.URL) (*http.Request, error) {
	uri.Path = path.Join("/", r.Params.Index, r.Params.Type, r.Params.Id, "_update")
	uri.RawQuery = r.Params.Values().Encode()

	buf := new(bytes.Buffer)

	if err := json.NewEncoder(buf).Encode(r.Source); err != nil {
		return nil, err
	}

	return http.NewRequest("POST", uri.String(), buf)
}

//
//
//

type BulkParams struct {
	Consistency string
	Refresh     string
	Replication string
}

func (p BulkParams) Values() url.Values {
	return values(map[string]string{
		"consistency": p.Consistency,
		"refresh":     p.Refresh,
		"replication": p.Replication,
	})
}

type BulkIndexable interface {
	EncodeBulkHeader(*json.Encoder) error
	EncodeSource(*json.Encoder) error
}

type BulkRequest struct {
	Params   BulkParams
	Requests []BulkIndexable
}

func (r BulkRequest) Request(uri *url.URL) (*http.Request, error) {
	uri.Path = "/_bulk"
	uri.RawQuery = r.Params.Values().Encode()

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)

	for _, req := range r.Requests {
		if err := req.EncodeBulkHeader(enc); err != nil {
			return nil, err
		}

		if err := req.EncodeSource(enc); err != nil {
			return nil, err
		}
	}

	return http.NewRequest("PUT", uri.String(), buf)
}
