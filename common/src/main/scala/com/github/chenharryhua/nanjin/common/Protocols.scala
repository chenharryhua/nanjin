package com.github.chenharryhua.nanjin.common

import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

import scala.collection.immutable

sealed abstract class HttpProtocols(override val entryName: String)
    extends EnumEntry with Product with Serializable

object HttpProtocols extends Enum[HttpProtocols] with CatsEnum[HttpProtocols] with CirceEnum[HttpProtocols] {
  override val values: immutable.IndexedSeq[HttpProtocols] = findValues

  case object HTTP extends HttpProtocols("http")
  case object HTTPS extends HttpProtocols("https")

  type HTTP = HTTP.type
  type HTTPS = HTTPS.type
}

sealed abstract class S3Protocols(override val entryName: String)
    extends EnumEntry with Product with Serializable

object S3Protocols extends Enum[S3Protocols] with CatsEnum[S3Protocols] with CirceEnum[S3Protocols] {
  override val values: immutable.IndexedSeq[S3Protocols] = findValues

  case object S3 extends S3Protocols("s3")

  type S3 = S3.type
}

sealed abstract class FtpProtocols(override val entryName: String)
    extends EnumEntry with Product with Serializable

object FtpProtocols extends Enum[FtpProtocols] with CatsEnum[FtpProtocols] with CirceEnum[FtpProtocols] {
  override val values: immutable.IndexedSeq[FtpProtocols] = findValues
  case object Ftp extends FtpProtocols("ftp")
  case object Sftp extends FtpProtocols("sftp")
  case object Ftps extends FtpProtocols("ftps")

  type Ftp = Ftp.type
  type Sftp = Sftp.type
  type Ftps = Ftps.type
}
