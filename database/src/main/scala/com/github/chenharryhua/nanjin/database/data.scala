package com.github.chenharryhua.nanjin.database

import cats.Show
import com.github.chenharryhua.nanjin.common.Secret

/** PostgreSQL connection credentials. For security the `password` is a `Secret`, so `toString`/`Show` mask it
  * as `***`; the raw value is used only via `password.value` when building the JDBC config.
  */
final case class Postgres(username: String, password: Secret, host: String, port: Int, database: String)
object Postgres:
  given Show[Postgres] = Show.fromToString
end Postgres

/** Amazon Redshift connection credentials. For security the `password` is a `Secret`, so `toString`/`Show`
  * mask it as `***`; the raw value is used only via `password.value` when building the JDBC config.
  */
final case class Redshift(username: String, password: Secret, host: String, port: Int, database: String)
object Redshift:
  given Show[Redshift] = Show.fromToString
end Redshift

/** SQL Server connection credentials. For security the `password` is a `Secret`, so `toString`/`Show` mask it
  * as `***`; the raw value is used only via `password.value` when building the JDBC config.
  */
final case class SqlServer(username: String, password: Secret, host: String, port: Int, database: String)
object SqlServer:
  given Show[SqlServer] = Show.fromToString
end SqlServer
