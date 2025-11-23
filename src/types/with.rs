/// Append something to Self.
///
/// For handily constructing message.
pub trait With<T> {
  fn with(self, sth: T) -> Self;
}
