package com.github.chenharryhua.nanjin.database

final case class Username(value: String) extends AnyVal
final case class Password(value: String) extends AnyVal
final case class Host(value: String) extends AnyVal
final case class Port(value: Int) extends AnyVal

final case class DatabaseName(value: String) extends AnyVal
final case class ConnectionString(value: String) extends AnyVal
final case class DriverString(value: String) extends AnyVal
