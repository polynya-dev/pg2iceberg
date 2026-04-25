//! Schema-name handling for the `_pg2iceberg` coordination namespace.
//!
//! The schema is user-configurable (parallel pg2iceberg instances on a single
//! PG can share a database via separate schemas). Schema names participate in
//! SQL identifiers, which can't be parameterized, so we sanitize at
//! construction time the same way `stream/coordinator_pg.go:417` does.

use std::fmt;

pub const DEFAULT_SCHEMA: &str = "_pg2iceberg";

/// A coord-namespace name that has been validated as a safe SQL identifier.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct CoordSchema(String);

impl CoordSchema {
    /// Default schema name (`_pg2iceberg`).
    pub fn default_name() -> Self {
        // Safe by construction.
        CoordSchema(DEFAULT_SCHEMA.to_string())
    }

    /// Sanitize an arbitrary input into a safe identifier. Lowercases ASCII
    /// letters; replaces anything that isn't `[a-z0-9_]` with `_`.
    /// Empty input maps to the default.
    pub fn sanitize(input: &str) -> Self {
        if input.is_empty() {
            return Self::default_name();
        }
        let mut out = String::with_capacity(input.len());
        for ch in input.chars() {
            let c = ch.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' {
                out.push(c);
            } else {
                out.push('_');
            }
        }
        CoordSchema(out)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Qualify a table within the schema, e.g. `"_pg2iceberg.log_seq"`.
    pub fn qualify(&self, table: &str) -> String {
        format!("{}.{}", self.0, table)
    }
}

impl fmt::Display for CoordSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_underscore_pg2iceberg() {
        assert_eq!(CoordSchema::default_name().as_str(), "_pg2iceberg");
    }

    #[test]
    fn empty_input_uses_default() {
        assert_eq!(CoordSchema::sanitize("").as_str(), "_pg2iceberg");
    }

    #[test]
    fn lowercases_and_keeps_word_chars() {
        assert_eq!(
            CoordSchema::sanitize("My_Pg2Iceberg_Test").as_str(),
            "my_pg2iceberg_test"
        );
    }

    #[test]
    fn replaces_unsafe_chars_with_underscore() {
        assert_eq!(
            CoordSchema::sanitize("a; DROP TABLE x; --").as_str(),
            "a__drop_table_x____"
        );
    }

    #[test]
    fn qualify_uses_dot() {
        let s = CoordSchema::default_name();
        assert_eq!(s.qualify("log_seq"), "_pg2iceberg.log_seq");
    }
}
