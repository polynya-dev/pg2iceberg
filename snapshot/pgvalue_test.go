package snapshot

import (
	"math/big"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func numeric(i int64, exp int32) pgtype.Numeric {
	return pgtype.Numeric{
		Int:   big.NewInt(i),
		Exp:   exp,
		Valid: true,
	}
}

// TestPgValueToString_Numeric pins the text format of a handful of tricky
// numerics. The regression that prompted this suite: `-0.01` was being
// rendered as `"0.-1"` because zero-padding was splitting the sign from the
// digits.
func TestPgValueToString_Numeric(t *testing.T) {
	cases := []struct {
		name string
		in   pgtype.Numeric
		want string
	}{
		{"positive_integer", numeric(123, 0), "123"},
		{"positive_with_trailing_zeros", numeric(12, 3), "12000"},
		{"positive_with_scale", numeric(12345, -2), "123.45"},
		{"negative_integer", numeric(-42, 0), "-42"},
		{"negative_with_scale", numeric(-12345, -2), "-123.45"},
		{"negative_less_than_one", numeric(-1, -2), "-0.01"},       // the bug
		{"negative_fractional_with_pad", numeric(-5, -3), "-0.005"},
		{"positive_less_than_one", numeric(1, -2), "0.01"},
		{"zero", numeric(0, 0), "0"},
		{"zero_scaled", numeric(0, -2), "0.00"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PgValueToString(tc.in, 1700 /* numeric oid */)
			if got != tc.want {
				t.Errorf("PgValueToString(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestPgValueToString_NumericInvalid(t *testing.T) {
	n := pgtype.Numeric{Valid: false}
	if got := PgValueToString(n, 1700); got != nil {
		t.Errorf("invalid numeric should be nil, got %v", got)
	}
}
