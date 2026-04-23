package iceberg

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/pg2iceberg/pg2iceberg/postgres"
)

func manifestTestSchema(pk []string) *postgres.TableSchema {
	return &postgres.TableSchema{
		Table: "public.orders",
		Columns: []postgres.Column{
			{Name: "id", PGType: postgres.Int8, IsNullable: false, FieldID: 1},
			{Name: "customer_id", PGType: postgres.Int8, IsNullable: false, FieldID: 2},
			{Name: "notes", PGType: postgres.Text, IsNullable: true, FieldID: 3},
		},
		PK: pk,
	}
}

func TestIcebergSchemaJSONString_EmitsIdentifierFieldIDs(t *testing.T) {
	ts := manifestTestSchema([]string{"id"})
	s := IcebergSchemaJSONString(ts)

	if !strings.Contains(s, `"identifier-field-ids":[1]`) {
		t.Fatalf("expected identifier-field-ids in output, got: %s", s)
	}

	// The string should still be valid JSON and should round-trip.
	var parsed map[string]any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, s)
	}
	if parsed["type"] != "struct" {
		t.Errorf("type = %v, want struct", parsed["type"])
	}
	ids, ok := parsed["identifier-field-ids"].([]any)
	if !ok || len(ids) != 1 || ids[0].(float64) != 1 {
		t.Errorf("identifier-field-ids = %v, want [1]", parsed["identifier-field-ids"])
	}
}

func TestIcebergSchemaJSONString_CompositeIdentifier(t *testing.T) {
	ts := manifestTestSchema([]string{"id", "customer_id"})
	s := IcebergSchemaJSONString(ts)
	if !strings.Contains(s, `"identifier-field-ids":[1,2]`) {
		t.Fatalf("expected composite identifier-field-ids, got: %s", s)
	}
}

func TestIcebergSchemaJSONString_OmitsIdentifierWhenNoPK(t *testing.T) {
	ts := manifestTestSchema(nil)
	s := IcebergSchemaJSONString(ts)
	if strings.Contains(s, "identifier-field-ids") {
		t.Fatalf("should not emit identifier-field-ids when PK is empty, got: %s", s)
	}
	// Still valid JSON.
	var parsed map[string]any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, s)
	}
}
