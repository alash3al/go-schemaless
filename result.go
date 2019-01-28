package schemaless

// Result ...
type Result struct {
	Total uint64      `json:"total"`
	Hits  []*Document `json:"hits"`
	Pager Pager       `json:"pager"`
}

// Pager pagination info
type Pager struct {
	Pages   uint64 `json:"pages"`
	Current int64  `json:"current"`
	Next    int64  `json:"next"`
	Prev    int64  `json:"prev"`
}
