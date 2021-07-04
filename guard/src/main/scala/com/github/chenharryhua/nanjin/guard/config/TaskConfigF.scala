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

@Lenses final case class SlackColor(succ: String, fail: String, warn: String, info: String)

@Lenses final case class TaskParams private (
  appName: String,
  zoneId: ZoneId,
  dailySummaryReset: Int, // 0 - 23,
  color: SlackColor,
  hostName: String
)

object TaskParams {

  def apply(appName: String, hostName: HostName): TaskParams = TaskParams(
    appName = appName,
    zoneId = ZoneId.systemDefault(),
    dailySummaryReset = 0, // midnight
    color = SlackColor(succ = "good", fail = "danger", warn = "#ffd699", info = "#b3d1ff"),
    hostName = hostName.name
  )
}

sealed private[guard] trait TaskConfigF[F]

private object TaskConfigF {
  implicit val functorTaskConfigF: Functor[TaskConfigF] = cats.derived.semiauto.functor[TaskConfigF]

  final case class InitParams[K](applicationName: String, hostName: HostName) extends TaskConfigF[K]

  final case class WithZoneId[K](value: ZoneId, cont: K) extends TaskConfigF[K]
  final case class WithDailySummaryReset[K](value: Int, cont: K) extends TaskConfigF[K]

  final case class WithSlackSuccColor[K](value: String, cont: K) extends TaskConfigF[K]
  final case class WithSlackFailColor[K](value: String, cont: K) extends TaskConfigF[K]
  final case class WithSlackWarnColor[K](value: String, cont: K) extends TaskConfigF[K]
  final case class WithSlackInfoColor[K](value: String, cont: K) extends TaskConfigF[K]

  final case class WithHostName[K](value: HostName, cont: K) extends TaskConfigF[K]

  val algebra: Algebra[TaskConfigF, TaskParams] =
    Algebra[TaskConfigF, TaskParams] {
      case InitParams(appName, hostName) => TaskParams(appName, hostName)
      case WithZoneId(v, c)              => TaskParams.zoneId.set(v)(c)
      case WithDailySummaryReset(v, c)   => TaskParams.dailySummaryReset.set(v)(c)
      case WithSlackSuccColor(v, c)      => TaskParams.color.composeLens(SlackColor.succ).set(v)(c)
      case WithSlackFailColor(v, c)      => TaskParams.color.composeLens(SlackColor.fail).set(v)(c)
      case WithSlackWarnColor(v, c)      => TaskParams.color.composeLens(SlackColor.warn).set(v)(c)
      case WithSlackInfoColor(v, c)      => TaskParams.color.composeLens(SlackColor.info).set(v)(c)
      case WithHostName(v, c)            => TaskParams.hostName.set(v.name)(c)
    }
}

final case class TaskConfig private (value: Fix[TaskConfigF]) {
  import TaskConfigF._

  def zone_id(zoneId: ZoneId): TaskConfig = TaskConfig(Fix(WithZoneId(zoneId, value)))

  def daily_summary_reset(hour: Refined[Int, And[GreaterEqual[W.`0`.T], LessEqual[W.`23`.T]]]): TaskConfig =
    TaskConfig(Fix(WithDailySummaryReset(hour.value, value)))

  def slack_succ_color(v: String): TaskConfig = TaskConfig(Fix(WithSlackSuccColor(v, value)))
  def slack_fail_color(v: String): TaskConfig = TaskConfig(Fix(WithSlackFailColor(v, value)))
  def slack_warn_color(v: String): TaskConfig = TaskConfig(Fix(WithSlackWarnColor(v, value)))
  def slack_info_color(v: String): TaskConfig = TaskConfig(Fix(WithSlackInfoColor(v, value)))

  def host_name(hostName: HostName): TaskConfig = TaskConfig(Fix(WithHostName(hostName, value)))

  def evalConfig: TaskParams = scheme.cata(algebra).apply(value)
}

private[guard] object TaskConfig {

  def apply(appName: String, hostName: HostName): TaskConfig = new TaskConfig(
    Fix(TaskConfigF.InitParams[Fix[TaskConfigF]](appName, hostName)))
}
