package schemaless

// FilterOpts ...
type FilterOpts struct {
	Where string
	Args  map[string]interface{}
	Limit int64
}
