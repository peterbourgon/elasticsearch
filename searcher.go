package elasticsearch

// Searcher is the interface that wraps the basic Search method.
// Search transforms a Request into a SearchResponse (or an error).
type Searcher interface {
	Search(SearchRequest) (SearchResponse, error)
}

// MultiSearcher is the interface that wraps the MultiSearch method.
type MultiSearcher interface {
	MultiSearch(MultiSearchRequest) (MultiSearchResponse, error)
}
