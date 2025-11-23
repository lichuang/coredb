use std::fmt::Debug;
use std::fmt::Formatter;

/// An operation that updates a field, delete it.
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub enum Operation<T> {
  Update(T),
  Delete,
}

impl<T> Debug for Operation<T> {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
    match self {
      Operation::Update(_) => f.debug_tuple("Update").field(&"[binary]").finish(),
      Operation::Delete => f.debug_tuple("Delete").finish(),
    }
  }
}
