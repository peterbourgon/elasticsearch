package elasticsearch

// SearchResponse represents the response given by ElasticSearch from a search
// query.
type SearchResponse struct {
	Took int `json:"took"` // ms

	HitsWrapper struct {
		Total int `json:"total"`
		Hits  []struct {
			Index string   `json:"_index"`
			Type  string   `json:"_type"`
			ID    string   `json:"_id"`
			Score *float64 `json:"_score"` // can be 'null' with constant_score
		} `json:"hits,omitempty"`
	} `json:"hits"`

	Facets map[string]FacetResponse `json:"facets,omitempty"`

	TimedOut bool   `json:"timed_out,omitempty"`
	Error    string `json:"error,omitempty"`
	Status   int    `json:"status,omitempty"`
}

type FacetResponse struct {
	Type    string `json:"_type"`
	Missing int64  `json:"missing"`
	Total   int64  `json:"total"`
	Other   int64  `json:"other"`
	Terms   []struct {
		Term  string `json:"term"`
		Count int64  `json:"count"`
	} `json:"terms"`
}

type MultiSearchResponse struct {
	Responses []SearchResponse `json:"responses"`
}
