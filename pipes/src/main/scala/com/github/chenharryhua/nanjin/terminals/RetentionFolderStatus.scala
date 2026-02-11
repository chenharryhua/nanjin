package com.github.chenharryhua.nanjin.terminals

import cats.kernel.Order
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.generic.JsonCodec
import io.lemonlabs.uri.Url

sealed trait RetentionStatus extends EnumEntry

object RetentionStatus extends Enum[RetentionStatus] with CirceEnum[RetentionStatus] {
  val values: IndexedSeq[RetentionStatus] = findValues

  case object Removed extends RetentionStatus
  case object RemovalFailed extends RetentionStatus
  case object Retained extends RetentionStatus
}

@JsonCodec
final case class RetentionFolderStatus(folder: Url, status: RetentionStatus)
object RetentionFolderStatus {
  implicit val orderingRetentionFolderStatus: Ordering[RetentionFolderStatus] =
    Ordering.by(_.folder.toString())
  implicit val orderRetentionFolderStatus: Order[RetentionFolderStatus] = Order.fromOrdering
}
