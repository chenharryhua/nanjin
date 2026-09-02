package com.github.chenharryhua.nanjin.http.client.auth

import cats.Show

/** A credential secret (client secret, password, or authorization code) whose value is masked in every
  * rendered form.
  *
  * '''Security:''' a plain `String` secret leaks into logs, exception messages, and debug output through the
  * default `toString` of any structure that holds it, and into JSON if the structure derives a codec.
  * Wrapping it here prevents that at the type level:
  *   - `toString` and `Show` render `***`, never the value;
  *   - because this is a real runtime class (not an `opaque type`, which erases and would let the enclosing
  *     case class print the underlying `String`), the masking holds wherever the value is rendered, including
  *     the default `toString` of the credential case classes that embed it;
  *   - there is deliberately '''no''' JSON `Encoder`/`Codec`, which also prevents the enclosing credential
  *     types from deriving one, so a secret can never be serialized in cleartext.
  *
  * The raw value is reachable only through `value` — the single, intentional cleartext escape hatch, used
  * when building the token request sent to the authorization server.
  */
final class Password(val value: String) {
  override def toString: String = "***"
  override def equals(other: Any): Boolean = other match {
    case that: Password => value.equals(that.value)
    case _              => false
  }
  override def hashCode: Int = value.hashCode
}
object Password {
  def apply(value: String): Password = new Password(value)

  /** Masks the value for security; never renders the secret. */
  given Show[Password] = _ => "***"

  /** Ergonomic construction from a string literal at call sites. */
  given Conversion[String, Password] = Password(_)
}
