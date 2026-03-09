package com.github.chenharryhua.nanjin.common

import io.github.iltotore.iron.constraint.all.{Blank, ValidURL}
import io.github.iltotore.iron.constraint.any.DescribedAs
import io.github.iltotore.iron.constraint.collection.MaxLength
import io.github.iltotore.iron.constraint.numeric.Positive
import io.github.iltotore.iron.constraint.string.Trimmed
import io.github.iltotore.iron.{:|, Not}

object database {
  type Username = String :| DescribedAs[Not[Blank] & Trimmed, ""]

  type Password = String :| DescribedAs[Not[Blank] & Trimmed, ""]

  type DatabaseName = String :| DescribedAs[Not[Blank] & MaxLength[128], ""]

  type TableName = String :| DescribedAs[Not[Blank] & MaxLength[128], ""]

  type Host = String :| ValidURL

  type Port = Int :| Positive

  final case class Postgres(
    username: Username,
    password: Password,
    host: Host,
    port: Port,
    database: DatabaseName)

  final case class Redshift(
    username: Username,
    password: Password,
    host: Host,
    port: Port,
    database: DatabaseName)

  final case class SqlServer(
    username: Username,
    password: Password,
    host: Host,
    port: Port,
    database: DatabaseName)

}
