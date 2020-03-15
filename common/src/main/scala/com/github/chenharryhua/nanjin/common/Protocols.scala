package com.github.chenharryhua.nanjin.common

import enumeratum.{CatsEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class Protocols(val value: String) extends EnumEntry with Serializable {}

object Protocols extends Enum[Protocols] with CatsEnum[Protocols] {
  override val values: immutable.IndexedSeq[Protocols] = findValues
  case object S3 extends Protocols("S3")
  case object S3A extends Protocols("S3A")
  case object SASL_SSL extends Protocols("SASL_SSL")
  case object PLAINTEXT extends Protocols("PLAINTEXT")
  case object SASL_PLAINTEXT extends Protocols("SASL_PLAINTEXT")
  case object SSL extends Protocols("SSL")
  case object HTTP extends Protocols("HTTP")
  case object HTTPS extends Protocols("HTTPS")
  case object MongoDB extends Protocols("mongodb")
  case object Postgres extends Protocols("jdbc:postgresql")
  case object Redshift extends Protocols("jdbc:redshift")
  case object SqlServer extends Protocols("jdbc:sqlserver")
  case object Bolt extends Protocols("bolt")
  case object FTP extends Protocols("ftp")

  type S3             = S3.type
  type S3A            = S3A.type
  type SASL_SSL       = SASL_SSL.type
  type PLAINTEXT      = PLAINTEXT.type
  type SASL_PLAINTEXT = SASL_PLAINTEXT.type
  type SSL            = SSL.type
  type HTTP           = HTTP.type
  type HTTPS          = HTTPS.type
  type MongoDB        = MongoDB.type
  type Postgres       = Postgres.type
  type Redshift       = Redshift.type
  type SqlServer      = SqlServer.type
  type Bolt           = Bolt.type
  type FTP            = FTP.type
}
