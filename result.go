package schemaless

// Result ...
type Result struct {
	Total uint64     `json:"total"`
	Hits  []Document `json:"hits"`
}
