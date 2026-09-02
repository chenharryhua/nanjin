package com.github.chenharryhua.nanjin.common

import cats.Show

/** A sensitive value (password, client secret, authorization code, token, ...) whose contents are masked in
  * every rendered form.
  *
  * '''Security:''' a plain `String` secret leaks into logs, exception messages, and debug output through the
  * default `toString` of any structure that holds it, and into JSON if that structure derives a codec.
  * Wrapping it here prevents that at the type level:
  *   - `toString` and `Show` render `***`, never the value;
  *   - because this is a real runtime class (not an `opaque type`, which erases and would let an enclosing
  *     case class print the underlying `String`), the masking holds wherever the value is rendered, including
  *     the default `toString` of case classes that embed it;
  *   - there is deliberately '''no''' JSON `Encoder`/`Codec`, which also prevents enclosing types from
  *     deriving one, so a secret can never be serialized in cleartext.
  *
  * The raw value is reachable only through `value` — the single, intentional cleartext escape hatch, used at
  * trust boundaries (e.g. building a JDBC config or a token request).
  */
final class Secret(val value: String) {
  override def toString: String = "***"
  override def equals(other: Any): Boolean = other match {
    case that: Secret => value.equals(that.value)
    case _            => false
  }
  override def hashCode: Int = value.hashCode
}
object Secret {
  def apply(value: String): Secret = new Secret(value)

  /** Masks the value for security; never renders the secret. */
  given Show[Secret] = _ => "***"

  /** Ergonomic construction from a string literal at call sites. */
  given Conversion[String, Secret] = Secret(_)
}
