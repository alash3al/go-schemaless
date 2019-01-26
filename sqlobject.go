package schemaless

import (
	"database/sql/driver"
	"encoding/json"
)

// SQLObject - implements a driver custom type for postgress
type SQLObject map[string]interface{}

// String - return the json string of the object
func (o SQLObject) String() string {
	j, _ := json.Marshal(o)

	return string(j)
}

// Value - implements driver.Value
func (o SQLObject) Value() (driver.Value, error) {
	jsonBytes, err := json.Marshal(o)
	return jsonBytes, err
}

// Scan - implements driver.Scan
func (o *SQLObject) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return nil
	}

	var v map[string]interface{}

	err := json.Unmarshal(source, &v)
	if err != nil {
		return err
	}

	*o = v

	return nil
}
