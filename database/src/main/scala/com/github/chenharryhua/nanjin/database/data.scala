package com.github.chenharryhua.nanjin.database

import com.github.chenharryhua.nanjin.common.IronRefined.PlusConversion
import io.github.iltotore.iron.constraint.all.{Blank, ValidURL}
import io.github.iltotore.iron.constraint.any.DescribedAs
import io.github.iltotore.iron.constraint.collection.MaxLength
import io.github.iltotore.iron.constraint.numeric.Positive
import io.github.iltotore.iron.constraint.string.Trimmed
import io.github.iltotore.iron.{Not, RefinedType}

private type NBTC = DescribedAs[Not[Blank] & Trimmed, "no blank, trimmed"]
type Username = Username.T
object Username extends RefinedType[String, NBTC] with PlusConversion[String, NBTC]

type Password = Password.T
object Password extends RefinedType[String, NBTC] with PlusConversion[String, NBTC]

private type NBMC = DescribedAs[Not[Blank] & MaxLength[128], "no blank, max=128"]
type Database = Database.T
object Database extends RefinedType[String, NBMC] with PlusConversion[String, NBMC]

type Host = Host.T
object Host extends RefinedType[String, ValidURL] with PlusConversion[String, ValidURL]

type Port = Port.T
object Port extends RefinedType[Int, Positive] with PlusConversion[Int, Positive]

type TableName = TableName.T
object TableName extends RefinedType[String, NBMC] with PlusConversion[String, NBMC]

final case class Postgres(username: Username, password: Password, host: Host, port: Port, database: Database)

final case class Redshift(username: Username, password: Password, host: Host, port: Port, database: Database)

final case class SqlServer(username: Username, password: Password, host: Host, port: Port, database: Database)
