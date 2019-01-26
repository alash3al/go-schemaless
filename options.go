package schemaless

// FilterOpts ...
type FilterOpts struct {
	Where  string
	Args   map[string]interface{}
	Offset int64
	Limit  int64
}
