package com.github.chenharryhua.nanjin.database

final case class Postgres(username: String, password: String, host: String, port: Int, database: String)

final case class Redshift(username: String, password: String, host: String, port: Int, database: String)

final case class SqlServer(username: String, password: String, host: String, port: Int, database: String)
