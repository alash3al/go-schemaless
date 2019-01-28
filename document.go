package schemaless

// DocumentReservedKeys ...
var DocumentReservedKeys = map[string]bool{
	"uuid":       true,
	"collection": true,
	"updated_at": true,
	"created_at": true,
	"deleted_at": true,
}

// Document - represents a datastore document
type Document struct {
	UUID       string               `json:"uuid" db:"uuid"`
	Collection string               `json:"collection" db:"collection"`
	UpdatedAt  int64                `json:"updated_at" db:"updated_at"`
	CreatedAt  int64                `json:"created_at" db:"created_at"`
	DeletedAt  int64                `json:"deleted_at" db:"deleted_at"`
	Data       SQLObject            `json:"data" db:"data"`
	Relations  map[string]*Document `json:"relations" db:"-"`
}

// Deleted - whether the documented deleted or not
func (d Document) Deleted() bool {
	return d.DeletedAt > 0
}
