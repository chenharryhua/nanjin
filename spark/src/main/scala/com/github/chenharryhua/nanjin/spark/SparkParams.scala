package com.github.chenharryhua.nanjin.spark

import monocle.macros.Lenses

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

final case class StorageRootPath(value: String) extends AnyVal

@Lenses final case class UploadRate(batchSize: Int, duration: FiniteDuration)

object UploadRate {
  val default: UploadRate = UploadRate(1000, 1.second)
}

final case class Username(value: String) extends AnyVal
final case class Password(value: String) extends AnyVal
final case class Host(value: String) extends AnyVal
final case class Port(value: Int) extends AnyVal

final case class DatabaseName(value: String) extends AnyVal
final case class ConnectionString(value: String) extends AnyVal
final case class DriverString(value: String) extends AnyVal
