package com.github.chenharryhua.nanjin.guard.config

import cats.Functor
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.boolean.And
import eu.timepit.refined.numeric.{GreaterEqual, LessEqual}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import java.time.ZoneId

@Lenses final case class TaskParams private (
  appName: String,
  zoneId: ZoneId,
  dailySummaryReset: Int // 0 - 23
)

object TaskParams {

  def apply(appName: String): TaskParams = TaskParams(
    appName = appName,
    zoneId = ZoneId.systemDefault(),
    dailySummaryReset = 0 // midnight
  )
}

sealed private[guard] trait TaskConfigF[F]

private object TaskConfigF {
  implicit val functorServiceConfigF: Functor[TaskConfigF] = cats.derived.semiauto.functor[TaskConfigF]

  final case class InitParams[K](applicationName: String) extends TaskConfigF[K]

  final case class WithZoneId[K](value: ZoneId, cont: K) extends TaskConfigF[K]
  final case class WithDailySummaryReset[K](value: Int, cont: K) extends TaskConfigF[K]

  val algebra: Algebra[TaskConfigF, TaskParams] =
    Algebra[TaskConfigF, TaskParams] {
      case InitParams(appName)         => TaskParams(appName)
      case WithZoneId(v, c)            => TaskParams.zoneId.set(v)(c)
      case WithDailySummaryReset(v, c) => TaskParams.dailySummaryReset.set(v)(c)
    }
}

final case class TaskConfig private (value: Fix[TaskConfigF]) {
  import TaskConfigF._

  def withZoneId(zoneId: ZoneId): TaskConfig =
    TaskConfig(Fix(WithZoneId(zoneId, value)))

  def withDailySummaryReset(hour: Refined[Int, And[GreaterEqual[W.`0`.T], LessEqual[W.`23`.T]]]): TaskConfig =
    TaskConfig(Fix(WithDailySummaryReset(hour.value, value)))

  def evalConfig: TaskParams = scheme.cata(algebra).apply(value)
}

private[guard] object TaskConfig {

  def apply(appName: String): TaskConfig = new TaskConfig(Fix(TaskConfigF.InitParams[Fix[TaskConfigF]](appName)))
}
