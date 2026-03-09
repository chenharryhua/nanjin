package com.github.chenharryhua.nanjin.terminals

import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.lemonlabs.uri.Url
import io.circe.Codec

sealed trait RetentionStatus extends EnumEntry

object RetentionStatus
    extends Enum[RetentionStatus] with CirceEnum[RetentionStatus] with CatsEnum[RetentionStatus] {
  val values: IndexedSeq[RetentionStatus] = findValues

  case object Removed extends RetentionStatus
  case object RemovalFailed extends RetentionStatus
  case object Retained extends RetentionStatus
}

final case class RetentionFolderStatus(folder: Url, status: RetentionStatus) derives Codec.AsObject
