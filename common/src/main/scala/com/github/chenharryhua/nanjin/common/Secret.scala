package com.github.chenharryhua.nanjin.common

import cats.Show
import io.circe.{Encoder, Json}

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
  *   - the JSON `Encoder` is a '''masking''' encoder that emits `"***"`, so an enclosing type can derive a
  *     codec for logging without ever serializing the secret in cleartext. There is deliberately '''no'''
  *     `Decoder`: a secret cannot be recovered from its mask, so a masked value can never round-trip back
  *     into a real secret.
  *
  * The raw value is reachable only through `value` — the single, intentional cleartext escape hatch, used at
  * trust boundaries (e.g. building a JDBC config or a token request). Because the encoder masks everywhere, a
  * wire request that needs the real secret must take it from `value`, not from JSON.
  */
final class Secret(val value: String) {
  // The single source of the mask; Show and Encoder both derive from this.
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
  given Show[Secret] = _.toString

  /** Masking JSON encoder for logs and display: always emits the masked `toString`, never the secret. It lets
    * an enclosing type derive a codec safely; on any serialization path (including a wire payload) the field
    * is masked, so a request needing the real value must read it from `value`. There is intentionally no
    * `Decoder`.
    */
  given Encoder[Secret] = s => Json.fromString(s.toString)

}
