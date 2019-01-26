package schemaless

import (
	"database/sql"
	"sync"
)

// Manager - datastores manager
type Manager struct {
	stores *sync.Map
	db     *sql.DB
}

// NewManager - creates a new manager
func NewManager(db *sql.DB) *Manager {
	m := new(Manager)
	m.db = db
	m.stores = new(sync.Map)

	return m
}

// Get a datastore from the manager
func (m *Manager) Get(name string) (*Datastore, error) {
	store, found := m.stores.Load(name)
	if found {
		return store.(*Datastore), nil
	}

	s, e := NewDatastore(name, m.db)
	if e != nil {
		return nil, e
	}

	m.stores.Store(name, s)

	return s, nil
}
