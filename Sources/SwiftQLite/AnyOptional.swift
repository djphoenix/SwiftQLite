protocol AnyOptional {
  static var wrappedType: Any.Type { get }
}

extension Optional : AnyOptional {
  @inlinable static var wrappedType: Any.Type {
    return Wrapped.self
  }
}
