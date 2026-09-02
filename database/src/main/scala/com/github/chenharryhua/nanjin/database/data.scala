package com.github.chenharryhua.nanjin.database

import cats.Show

/** A database password whose value is masked in every rendered form.
  *
  * '''Security:''' a plain `String` password leaks into logs, exception messages, and debug output through
  * the default `toString` of any structure that holds it. Wrapping it here prevents that at the type level:
  *   - `toString` and `Show` render `***`, never the value;
  *   - because this is a real runtime class (not an `opaque type`, which erases and would let the enclosing
  *     case class print the underlying `String`), the masking holds wherever the value is rendered, including
  *     the default `toString` of the credential case classes that embed it;
  *   - there is deliberately '''no''' JSON `Encoder`/`Codec`, so the secret cannot be serialized in
  *     cleartext.
  *
  * The raw value is reachable only through `value` — the single, intentional cleartext escape hatch, used at
  * the JDBC boundary.
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

/** PostgreSQL connection credentials. For security the `password` is a `Password`, so `toString`/`Show` mask
  * it as `***`; the raw value is used only via `password.value` when building the JDBC config.
  */
final case class Postgres(username: String, password: Password, host: String, port: Int, database: String)
object Postgres:
  given Show[Postgres] = Show.fromToString
end Postgres

/** Amazon Redshift connection credentials. For security the `password` is a `Password`, so `toString`/ `Show`
  * mask it as `***`; the raw value is used only via `password.value` when building the JDBC config.
  */
final case class Redshift(username: String, password: Password, host: String, port: Int, database: String)
object Redshift:
  given Show[Redshift] = Show.fromToString
end Redshift

/** SQL Server connection credentials. For security the `password` is a `Password`, so `toString`/`Show` mask
  * it as `***`; the raw value is used only via `password.value` when building the JDBC config.
  */
final case class SqlServer(username: String, password: Password, host: String, port: Int, database: String)
object SqlServer:
  given Show[SqlServer] = Show.fromToString
end SqlServer
