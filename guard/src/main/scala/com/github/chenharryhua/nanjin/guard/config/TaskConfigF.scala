package com.github.chenharryhua.nanjin.guard.config

import cats.Functor
import com.github.chenharryhua.nanjin.common.HostName
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
  summaryResetAt: Int, // 0 - 23
  hostName: String)

object TaskParams {

  def apply(appName: String, hostName: HostName): TaskParams = TaskParams(
    appName = appName,
    zoneId = ZoneId.systemDefault(),
    summaryResetAt = 0, // midnight
    hostName = hostName.name
  )
}

sealed private[guard] trait TaskConfigF[F]

private object TaskConfigF {
  implicit val functorTaskConfigF: Functor[TaskConfigF] = cats.derived.semiauto.functor[TaskConfigF]

  final case class InitParams[K](applicationName: String, hostName: HostName) extends TaskConfigF[K]

  final case class WithZoneId[K](value: ZoneId, cont: K) extends TaskConfigF[K]

  final case class WithSummaryResetAt[K](value: Int, cont: K) extends TaskConfigF[K]

  final case class WithHostName[K](value: HostName, cont: K) extends TaskConfigF[K]

  val algebra: Algebra[TaskConfigF, TaskParams] =
    Algebra[TaskConfigF, TaskParams] {
      case InitParams(appName, hostName) => TaskParams(appName, hostName)
      case WithZoneId(v, c)              => TaskParams.zoneId.set(v)(c)
      case WithSummaryResetAt(v, c)      => TaskParams.summaryResetAt.set(v)(c)
      case WithHostName(v, c)            => TaskParams.hostName.set(v.name)(c)
    }
}

final case class TaskConfig private (value: Fix[TaskConfigF]) {
  import TaskConfigF.*

  def withZoneId(zoneId: ZoneId): TaskConfig = TaskConfig(Fix(WithZoneId(zoneId, value)))

  def withDailySummaryReset(hour: Refined[Int, And[GreaterEqual[W.`0`.T], LessEqual[W.`23`.T]]]): TaskConfig =
    TaskConfig(Fix(WithSummaryResetAt(hour.value, value)))

  def withHostName(hostName: HostName): TaskConfig = TaskConfig(Fix(WithHostName(hostName, value)))

  def evalConfig: TaskParams = scheme.cata(algebra).apply(value)
}

private[guard] object TaskConfig {

  def apply(appName: String, hostName: HostName): TaskConfig = new TaskConfig(
    Fix(TaskConfigF.InitParams[Fix[TaskConfigF]](appName, hostName)))
}
