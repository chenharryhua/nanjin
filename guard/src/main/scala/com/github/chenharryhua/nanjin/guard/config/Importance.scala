package com.github.chenharryhua.nanjin.guard.config

import cats.{Order, Show}
import cron4s.CronExpr
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}
import io.circe.generic.JsonCodec
import org.typelevel.cats.time.instances.duration

import java.time.Duration

sealed abstract class Importance(val value: Int) extends EnumEntry with Product

object Importance extends CatsEnum[Importance] with Enum[Importance] with CirceEnum[Importance] {
  override val values: IndexedSeq[Importance] = findValues

  case object Critical extends Importance(40)
  case object High extends Importance(30)
  case object Medium extends Importance(20)
  case object Low extends Importance(10)

  implicit final val orderingImportance: Ordering[Importance] = Ordering.by[Importance, Int](_.value)
  implicit final val orderImportance: Order[Importance]       = Order.fromOrdering[Importance]
}

@JsonCodec
sealed trait ScheduleType extends Product {
  final def fold[A](f: Duration => A, c: CronExpr => A): A = this match {
    case ScheduleType.Fixed(value) => f(value)
    case ScheduleType.Cron(value)  => c(value)
  }
}

object ScheduleType extends duration {
  implicit final val showSchedueType: Show[ScheduleType] = cats.derived.semiauto.show[ScheduleType]

  final case class Fixed(value: Duration) extends ScheduleType
  final case class Cron(value: CronExpr) extends ScheduleType
}
