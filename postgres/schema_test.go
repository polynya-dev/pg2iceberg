package postgres

import (
	"strings"
	"testing"
)

func sampleSchemaWithPK(pk []string) *TableSchema {
	return &TableSchema{
		Table: "public.orders",
		Columns: []Column{
			{Name: "id", PGType: Int8, IsNullable: false, FieldID: 1},
			{Name: "customer_id", PGType: Int8, IsNullable: false, FieldID: 2},
			{Name: "total", PGType: Numeric, IsNullable: true, FieldID: 3, Precision: 10, Scale: 2},
		},
		PK: pk,
	}
}

func TestValidate_RequiresPrimaryKey(t *testing.T) {
	ts := sampleSchemaWithPK(nil)
	err := ts.Validate()
	if err == nil {
		t.Fatalf("expected Validate to fail with no primary key, got nil")
	}
	if !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("error should mention primary key, got: %v", err)
	}
	if !strings.Contains(err.Error(), "public.orders") {
		t.Fatalf("error should name the offending table, got: %v", err)
	}
}

func TestValidate_AcceptsSimplePK(t *testing.T) {
	ts := sampleSchemaWithPK([]string{"id"})
	if err := ts.Validate(); err != nil {
		t.Fatalf("expected Validate to pass, got: %v", err)
	}
}

func TestValidate_AcceptsCompositePK(t *testing.T) {
	ts := sampleSchemaWithPK([]string{"id", "customer_id"})
	if err := ts.Validate(); err != nil {
		t.Fatalf("expected Validate to pass with composite PK, got: %v", err)
	}
}

func TestValidate_RejectsOverPrecisionNumeric(t *testing.T) {
	ts := sampleSchemaWithPK([]string{"id"})
	ts.Columns = append(ts.Columns, Column{
		Name:      "mega",
		PGType:    Numeric,
		FieldID:   4,
		Precision: 50,
		Scale:     0,
	})
	err := ts.Validate()
	if err == nil {
		t.Fatalf("expected Validate to fail on precision > 38")
	}
	if !strings.Contains(err.Error(), "precision") {
		t.Fatalf("error should mention precision, got: %v", err)
	}
}

func TestIcebergSchemaJSONWithID_EmitsIdentifierFieldIDs(t *testing.T) {
	ts := sampleSchemaWithPK([]string{"id"})
	s := IcebergSchemaJSONWithID(ts, 7)

	if got := s["schema-id"]; got != 7 {
		t.Errorf("schema-id = %v, want 7", got)
	}
	ids, ok := s["identifier-field-ids"].([]int)
	if !ok {
		t.Fatalf("identifier-field-ids missing or wrong type: %T (%v)", s["identifier-field-ids"], s["identifier-field-ids"])
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("identifier-field-ids = %v, want [1]", ids)
	}
}

func TestIcebergSchemaJSONWithID_CompositeIdentifierFieldIDs(t *testing.T) {
	ts := sampleSchemaWithPK([]string{"id", "customer_id"})
	s := IcebergSchemaJSONWithID(ts, 0)
	ids, ok := s["identifier-field-ids"].([]int)
	if !ok {
		t.Fatalf("identifier-field-ids missing or wrong type")
	}
	// Order should follow ts.PK order, which is the order the caller discovered.
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Errorf("identifier-field-ids = %v, want [1, 2]", ids)
	}
}

func TestIcebergSchemaJSONWithID_OmitsIdentifierWhenNoPK(t *testing.T) {
	ts := sampleSchemaWithPK(nil)
	s := IcebergSchemaJSONWithID(ts, 0)
	if _, present := s["identifier-field-ids"]; present {
		t.Errorf("identifier-field-ids should not be emitted when PK is empty")
	}
}
