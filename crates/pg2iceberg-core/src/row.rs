//! A row: its columns' values, by column name.
//!
//! The names live apart from the values, in a [`Columns`] list rows of one
//! relation share: building a row allocates its values only, not a string
//! and a tree node per column, and a reader taking one column from many
//! rows can find it once per list ([`Row::columns`], [`Row::value_at`])
//! instead of once per row.
//!
//! Otherwise it reads like the `BTreeMap<ColumnName, PgValue>` it replaces:
//! ordered by column name, each name at most once.

use crate::event::ColumnName;
use crate::value::PgValue;
use serde::de::Deserializer;
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

/// Column names, ordered and each once, shared by the rows that have them.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Columns(Arc<[ColumnName]>);

impl Columns {
    /// `names` ordered, and the place each takes: `order[i]` is where the
    /// `i`th of `names` went. `None` if a name repeats.
    pub fn sorted(names: Vec<ColumnName>) -> Option<(Self, Vec<usize>)> {
        let mut by_name: Vec<(usize, ColumnName)> = names.into_iter().enumerate().collect();
        by_name.sort_by(|a, b| a.1.cmp(&b.1));
        if by_name.windows(2).any(|w| w[0].1 == w[1].1) {
            return None;
        }
        let mut order = vec![0; by_name.len()];
        for (place, (i, _)) in by_name.iter().enumerate() {
            order[*i] = place;
        }
        let names: Vec<ColumnName> = by_name.into_iter().map(|(_, n)| n).collect();
        Some((Self(names.into()), order))
    }

    pub fn as_slice(&self) -> &[ColumnName] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Where `name` is.
    pub fn position(&self, name: &ColumnName) -> Option<usize> {
        self.0.binary_search(name).ok()
    }

    /// Where the column named `name` is.
    pub fn position_of(&self, name: &str) -> Option<usize> {
        self.0.binary_search_by(|c| c.0.as_str().cmp(name)).ok()
    }

    /// Whether `other` is this very list, not just an equal one: what a
    /// reader caching positions per list checks.
    pub fn same(&self, other: &Columns) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// This very list's identity, while it's alive: equal for two
    /// [`Self::same`] lists only.
    pub fn id(&self) -> usize {
        Arc::as_ptr(&self.0) as *const () as usize
    }

    fn with(&self, place: usize, name: ColumnName) -> Self {
        let mut names = self.0.to_vec();
        names.insert(place, name);
        Self(names.into())
    }

    fn without(&self, place: usize) -> Self {
        let mut names = self.0.to_vec();
        names.remove(place);
        Self(names.into())
    }
}

impl Default for Columns {
    fn default() -> Self {
        Self(Arc::from([]))
    }
}

impl fmt::Debug for Columns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0.iter().map(|c| &c.0)).finish()
    }
}

/// A row: one value per column of its [`Columns`].
#[derive(Clone, Default)]
pub struct Row {
    columns: Columns,
    values: Vec<PgValue>,
}

impl Row {
    pub fn new() -> Self {
        Self::default()
    }

    /// The row of `values`, one per column of `columns`, in its order.
    pub fn with_columns(columns: Columns, values: Vec<PgValue>) -> Self {
        assert_eq!(columns.len(), values.len(), "one value per column");
        Self { columns, values }
    }

    pub fn columns(&self) -> &Columns {
        &self.columns
    }

    /// The value of the `i`th of [`Self::columns`].
    pub fn value_at(&self, i: usize) -> &PgValue {
        &self.values[i]
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, name: &ColumnName) -> Option<&PgValue> {
        self.columns.position(name).map(|i| &self.values[i])
    }

    pub fn get_mut(&mut self, name: &ColumnName) -> Option<&mut PgValue> {
        self.columns.position(name).map(|i| &mut self.values[i])
    }

    pub fn contains_key(&self, name: &ColumnName) -> bool {
        self.columns.position(name).is_some()
    }

    /// Set `name`'s value; the one it replaces, if it had one.
    pub fn insert(&mut self, name: ColumnName, value: PgValue) -> Option<PgValue> {
        match self.columns.0.binary_search(&name) {
            Ok(i) => Some(std::mem::replace(&mut self.values[i], value)),
            Err(place) => {
                self.columns = self.columns.with(place, name);
                self.values.insert(place, value);
                None
            }
        }
    }

    pub fn remove(&mut self, name: &ColumnName) -> Option<PgValue> {
        let i = self.columns.position(name)?;
        self.columns = self.columns.without(i);
        Some(self.values.remove(i))
    }

    /// `name`'s place in the row, filled or not.
    pub fn entry(&mut self, name: ColumnName) -> Entry<'_> {
        Entry { row: self, name }
    }

    /// Keep the columns `keep` says to.
    pub fn retain(&mut self, mut keep: impl FnMut(&ColumnName, &mut PgValue) -> bool) {
        let columns = std::mem::take(&mut self.columns);
        let values = std::mem::take(&mut self.values);
        *self = columns
            .as_slice()
            .iter()
            .cloned()
            .zip(values)
            .filter_map(|(name, mut value)| keep(&name, &mut value).then_some((name, value)))
            .collect();
    }

    /// Columns and values, ordered by column name.
    pub fn iter(&self) -> Iter<'_> {
        self.columns.as_slice().iter().zip(self.values.iter())
    }

    pub fn keys(&self) -> std::slice::Iter<'_, ColumnName> {
        self.columns.as_slice().iter()
    }

    pub fn values(&self) -> std::slice::Iter<'_, PgValue> {
        self.values.iter()
    }

    pub fn values_mut(&mut self) -> std::slice::IterMut<'_, PgValue> {
        self.values.iter_mut()
    }
}

/// A column's place in a row ([`Row::entry`]).
pub struct Entry<'a> {
    row: &'a mut Row,
    name: ColumnName,
}

impl<'a> Entry<'a> {
    pub fn or_insert(self, value: PgValue) -> &'a mut PgValue {
        self.or_insert_with(|| value)
    }

    pub fn or_insert_with(self, value: impl FnOnce() -> PgValue) -> &'a mut PgValue {
        if !self.row.contains_key(&self.name) {
            self.row.insert(self.name.clone(), value());
        }
        self.row.get_mut(&self.name).expect("inserted above")
    }
}

pub type Iter<'a> = std::iter::Zip<std::slice::Iter<'a, ColumnName>, std::slice::Iter<'a, PgValue>>;

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        (self.columns.same(&other.columns) || self.columns.as_slice() == other.columns.as_slice())
            && self.values == other.values
    }
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl FromIterator<(ColumnName, PgValue)> for Row {
    /// As a map collects: a name given twice keeps its last value.
    fn from_iter<I: IntoIterator<Item = (ColumnName, PgValue)>>(iter: I) -> Self {
        let map: BTreeMap<ColumnName, PgValue> = iter.into_iter().collect();
        let (names, values): (Vec<ColumnName>, Vec<PgValue>) = map.into_iter().unzip();
        Self {
            columns: Columns(names.into()),
            values,
        }
    }
}

impl<const N: usize> From<[(ColumnName, PgValue); N]> for Row {
    fn from(entries: [(ColumnName, PgValue); N]) -> Self {
        entries.into_iter().collect()
    }
}

/// As a map indexes: panics without the column.
impl std::ops::Index<&ColumnName> for Row {
    type Output = PgValue;

    fn index(&self, name: &ColumnName) -> &PgValue {
        self.get(name)
            .unwrap_or_else(|| panic!("no column {name:?} in the row"))
    }
}

impl Extend<(ColumnName, PgValue)> for Row {
    fn extend<I: IntoIterator<Item = (ColumnName, PgValue)>>(&mut self, iter: I) {
        for (name, value) in iter {
            self.insert(name, value);
        }
    }
}

impl IntoIterator for Row {
    type Item = (ColumnName, PgValue);
    type IntoIter = std::iter::Zip<std::vec::IntoIter<ColumnName>, std::vec::IntoIter<PgValue>>;

    fn into_iter(self) -> Self::IntoIter {
        let names: Vec<ColumnName> = self.columns.as_slice().to_vec();
        names.into_iter().zip(self.values)
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = (&'a ColumnName, &'a PgValue);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// As the map it replaces: `{"column": value, ...}`.
impl Serialize for Row {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (name, value) in self {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Row {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(BTreeMap::<ColumnName, PgValue>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(s: &str) -> ColumnName {
        ColumnName(s.into())
    }

    #[test]
    fn reads_like_a_map_ordered_by_name() {
        let mut r = Row::new();
        assert_eq!(r.insert(name("b"), PgValue::Int4(2)), None);
        assert_eq!(r.insert(name("a"), PgValue::Int4(1)), None);
        assert_eq!(
            r.insert(name("b"), PgValue::Int4(3)),
            Some(PgValue::Int4(2))
        );
        let entries: Vec<(&str, &PgValue)> = r.iter().map(|(k, v)| (k.0.as_str(), v)).collect();
        assert_eq!(
            entries,
            [("a", &PgValue::Int4(1)), ("b", &PgValue::Int4(3))]
        );
        assert_eq!(r.remove(&name("a")), Some(PgValue::Int4(1)));
        assert_eq!(r.get(&name("a")), None);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn rows_sharing_columns_equal_rows_built_by_name() {
        let (columns, order) = Columns::sorted(vec![name("b"), name("a")]).unwrap();
        assert_eq!(order, [1, 0]);
        let shared = Row::with_columns(columns, vec![PgValue::Int4(1), PgValue::Int4(2)]);
        let built: Row = [(name("b"), PgValue::Int4(2)), (name("a"), PgValue::Int4(1))]
            .into_iter()
            .collect();
        assert_eq!(shared, built);
        assert_eq!(shared.get(&name("b")), Some(&PgValue::Int4(2)));
    }

    #[test]
    fn a_repeated_name_isnt_a_column_list() {
        assert!(Columns::sorted(vec![name("a"), name("a")]).is_none());
    }

    #[test]
    fn changing_a_shared_row_leaves_the_others_be() {
        let (columns, _) = Columns::sorted(vec![name("a")]).unwrap();
        let one = Row::with_columns(columns.clone(), vec![PgValue::Int4(1)]);
        let mut two = Row::with_columns(columns, vec![PgValue::Int4(2)]);
        two.insert(name("b"), PgValue::Null);
        assert_eq!(one.len(), 1);
        assert_eq!(two.len(), 2);
    }

    #[test]
    fn serializes_as_the_map_it_replaces() {
        let r: Row = [(name("id"), PgValue::Int4(7))].into_iter().collect();
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"{"id":{"Int4":7}}"#);
        assert_eq!(serde_json::from_str::<Row>(&json).unwrap(), r);
    }
}
