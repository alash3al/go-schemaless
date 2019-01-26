package schemaless

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/satori/go.uuid"
)

// Datastore ...
type Datastore struct {
	name string
	db   *sqlx.DB
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
func (s *Datastore) Update(uuid string, data SQLObject, override bool) (*Document, error) {
	now := time.Now().UnixNano()
	dataSQL := `data || $1`
	if override {
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

// GetAll - fetch all documents using the specified options
func (s *Datastore) GetAll(opts *FilterOpts) (*Result, error) {
	result := &Result{}
	result.Hits = []Document{}

	if opts == nil {
		opts = &FilterOpts{}
	}

	opts.Where = strings.TrimSpace(opts.Where)
	if opts.Where != "" {
		opts.Where = ` WHERE (` + (opts.Where) + `) `
	}

	if opts.Limit < 1 {
		opts.Limit = 10
	}

	if err := s.db.Get(&result.Total, `SELECT count(uuid) as totals FROM `+(s.name)+opts.Where); err != nil {
		return nil, err
	}

	rows, err := s.db.NamedQuery(`SELECT * FROM `+(s.name)+opts.Where+` LIMIT `+strconv.FormatInt(opts.Limit, 10), opts.Args)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var doc Document
		if err := rows.StructScan(&doc); err != nil {
			return nil, err
		}
		result.Hits = append(result.Hits, doc)
	}

	return result, nil
}

// DB - returns the underlying sqlx.db
func (s *Datastore) DB() *sqlx.DB {
	return s.db
}

func (s *Datastore) boot() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS ` + (s.name) + ` (
			uuid varchar not null,
			collection varchar DEFAULT 'default',
			data jsonb DEFAULT null,
			created_at bigint DEFAULT '0',
			updated_at bigint DEFAULT '0',
			deleted_at bigint DEFAULT '0'
		);
		
		CREATE INDEX on ` + (s.name) + ` (collection);
		CREATE INDEX on ` + (s.name) + ` (uuid);
		CREATE INDEX on ` + (s.name) + ` (collection, updated_at);
		CREATE INDEX on ` + (s.name) + ` (collection, created_at);
		CREATE INDEX on ` + (s.name) + ` (collection, deleted_at);
		CREATE INDEX on ` + (s.name) + ` using gin(data);
	`)

	return err
}
