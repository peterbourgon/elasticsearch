package elasticsearch

// SearchResponse represents the response given by ElasticSearch from a search
// query.
type SearchResponse struct {
	Took int `json:"took"` // ms

	HitsWrapper struct {
		Total int `json:"total"`
		Hits  []struct {
			Index string  `json:"_index"`
			Type  string  `json:"_type"`
			ID    string  `json:"_id"`
			Score float64 `json:"_score"`
		} `json:"hits"`
	} `json:"hits"`

	Facets map[string]interface{} `json:"facets,omitempty"` // TODO better?

	TimedOut bool   `json:"timed_out,omitempty"`
	Error    string `json:"error,omitempty"`
	Status   int    `json:"status,omitempty"`
}
