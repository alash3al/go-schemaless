package schemaless

// FilterOpts ...
type FilterOpts struct {
	Where  string
	Args   map[string]interface{}
	Offset int64
	Limit  int64
}

// OffsetFromPage set the offset from the specified page number
func (o *FilterOpts) OffsetFromPage(page int64) {
	if o.Limit < 1 {
		o.Limit = 10
	}

	o.Offset = (page - 1) * o.Limit

	if o.Offset < 0 {
		o.Offset = 0
	}
}
