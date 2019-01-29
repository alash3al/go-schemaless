package schemaless

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/satori/go.uuid"
)

// Relation ...
type Relation struct {
	Collection string
	LocalKey   string
	RemoteKey  string
}

// Datastore ...
type Datastore struct {
	name      string
	db        *sqlx.DB
	relations map[string]*Relation
}

// NewDatastore ...
func NewDatastore(name string, conn *sql.DB) (*Datastore, error) {
	db := sqlx.NewDb(conn, "postgres")

	if err := db.Ping(); err != nil {
		return nil, err
	}

	s := new(Datastore)
	s.name = name
	s.db = db

	if err := s.boot(); err != nil {
		return nil, err
	}

	return s, nil
}

// Create - insert a new document
func (s *Datastore) Create(doc *Document) error {
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}

	now := time.Now().UnixNano()
	sql := `
		INSERT INTO ` + (s.name) + `(uuid, collection, data, created_at, updated_at, deleted_at)
		VALUES($1, $2, $3, $4, $5, $6)
	`

	doc.UUID = id.String()
	doc.CreatedAt = now
	doc.UpdatedAt = now

	if _, err := s.db.Exec(sql, id, doc.Collection, doc.Data, doc.CreatedAt, doc.UpdatedAt, 0); err != nil {
		return err
	}

	return nil
}

// Update a document using its uuid, also you can merge/override its content
func (s *Datastore) Update(uuid string, data SQLObject, replace bool) (*Document, error) {
	now := time.Now().UnixNano()
	dataSQL := `data || $1`
	if replace {
		dataSQL = `$1`
	}

	if _, err := s.db.Exec(`UPDATE `+(s.name)+` SET data = `+(dataSQL)+`, updated_at = $2 WHERE uuid = $3`, data.String(), now, uuid); err != nil {
		return nil, err
	}

	return s.Get(uuid)
}

// Get - fetches the document at the specified uuid
func (s *Datastore) Get(uuid string) (*Document, error) {
	var doc Document

	err := s.db.QueryRowx(`SELECT * FROM `+(s.name)+` WHERE uuid = $1`, uuid).StructScan(&doc)

	return &doc, err
}

// Filter - fetch all documents using the specified options
func (s *Datastore) Filter(opts *FilterOpts) (*Result, error) {
	result := &Result{}
	result.Hits = []*Document{}

	if opts == nil {
		opts = &FilterOpts{}
	}

	opts.Where = strings.TrimSpace(opts.Where)
	if opts.Where != "" {
		opts.Where = (opts.Where) + ` `
	}

	if opts.Limit < 1 {
		opts.Limit = 10
	}

	rows, err := s.db.NamedQuery(`SELECT count(uuid) as totals FROM `+(s.name)+` `+opts.Where, opts.Args)
	if err != nil {
		return nil, err
	}

	rows.Next()
	rows.Scan(&result.Total)

	sorter := ""
	if len(opts.Order) > 0 {
		sorter = " ORDER BY "
		sorts := []string{}
		for k, v := range opts.Order {
			if !DocumentReservedKeys[k] {
				k = "data->>'" + (k) + "'"
			}
			sorts = append(sorts, fmt.Sprintf("%s %s", k, strings.ToUpper(v)))
		}
		sorter += strings.Join(sorts, ", ")
	}

	sql := `SELECT * FROM ` + (s.name) + ` ` + opts.Where + sorter + ` OFFSET ` + strconv.FormatInt(opts.Offset, 10) + ` LIMIT ` + strconv.FormatInt(opts.Limit, 10)

	rows, err = s.db.NamedQuery(sql, opts.Args)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var doc Document
		if err := rows.StructScan(&doc); err != nil {
			return nil, err
		}
		doc.Relations = map[string][]*Document{}
		for relname, rel := range s.relations {
			if rel.Collection == doc.Collection {
				doc.Relations[relname] = s.loadDocRelation(&doc, rel)
			}
		}
		result.Hits = append(result.Hits, &doc)
	}

	s.pagerify(opts, result)

	return result, nil
}

// DB - returns the underlying sqlx.db
func (s *Datastore) DB() *sqlx.DB {
	return s.db
}

// Name - returns the datastore name
func (s *Datastore) Name() string {
	return s.name
}

// loadDocRelation - load the document relations
func (s *Datastore) loadDocRelation(doc *Document, rel *Relation) []*Document {
	var ret = []*Document{}
	var lft = ""
	var rght = doc.Data[rel.LocalKey]

	if DocumentReservedKeys[rel.RemoteKey] {
		lft = rel.RemoteKey
	} else {
		lft = "data->>'" + (rel.RemoteKey) + "'"
	}

	s.db.Select(&ret, `SELECT * FROM `+s.name+` WHERE collection = $1 AND `+lft+` = $2`, doc.Collection, rght)

	return ret
}

// pagerify add the pagination info to the result
func (s *Datastore) pagerify(o *FilterOpts, r *Result) {
	if o.Limit < 1 {
		o.Limit = 10
	}

	pages := (r.Total / uint64(o.Limit)) + 1
	currentpage := (o.Offset / o.Limit) + 1
	next := currentpage + 1
	prev := currentpage - 1

	if uint64(next) > pages {
		next = -1
	}

	if prev < 1 {
		prev = -1
	}

	if r.Total < 1 {
		pages = 0
	}

	r.Pager = Pager{
		Pages:   pages,
		Next:    next,
		Prev:    prev,
		Current: currentpage,
	}
}

// boot create the missing tables/indexes
func (s *Datastore) boot() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS ` + (s.name) + ` (
			uuid varchar not null primary key,
			collection varchar DEFAULT 'default',
			data jsonb DEFAULT null,
			created_at bigint DEFAULT '0',
			updated_at bigint DEFAULT '0',
			deleted_at bigint DEFAULT '0'
		);
		
		CREATE INDEX IF NOT EXISTS ` + (s.name) + `_index_collection on ` + (s.name) + ` (collection);
		CREATE INDEX IF NOT EXISTS ` + (s.name) + `_index_collection_updated_at on ` + (s.name) + ` (collection, updated_at);
		CREATE INDEX IF NOT EXISTS ` + (s.name) + `_index_collection_created_at on ` + (s.name) + ` (collection, created_at);
		CREATE INDEX IF NOT EXISTS ` + (s.name) + `_index_collection_deletd_at on ` + (s.name) + ` (collection, deleted_at);
		CREATE INDEX IF NOT EXISTS ` + (s.name) + `_index_gin_data on ` + (s.name) + ` using gin(data);
	`)

	return err
}
