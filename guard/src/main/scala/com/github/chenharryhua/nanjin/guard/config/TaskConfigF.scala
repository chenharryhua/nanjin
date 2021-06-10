package com.github.chenharryhua.nanjin.guard.config

import cats.Functor
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import java.time.ZoneId

@Lenses final case class TaskParams private (
  appName: String,
  zoneId: ZoneId
)

object TaskParams {

  def apply(appName: String): TaskParams = TaskParams(
    appName = appName,
    zoneId = ZoneId.systemDefault()
  )
}

sealed private[guard] trait TaskConfigF[F]

private object TaskConfigF {
  implicit val functorServiceConfigF: Functor[TaskConfigF] = cats.derived.semiauto.functor[TaskConfigF]

  final case class InitParams[K](applicationName: String) extends TaskConfigF[K]

  final case class WithZoneId[K](value: ZoneId, cont: K) extends TaskConfigF[K]

  val algebra: Algebra[TaskConfigF, TaskParams] =
    Algebra[TaskConfigF, TaskParams] {
      case InitParams(appName) => TaskParams(appName)
      case WithZoneId(v, c)    => TaskParams.zoneId.set(v)(c)
    }
}

final case class TaskConfig private (value: Fix[TaskConfigF]) {
  import TaskConfigF._

  def withZoneId(zoneId: ZoneId): TaskConfig =
    TaskConfig(Fix(WithZoneId(zoneId, value)))

  def evalConfig: TaskParams = scheme.cata(algebra).apply(value)
}

private[guard] object TaskConfig {

  def apply(appName: String): TaskConfig = new TaskConfig(Fix(TaskConfigF.InitParams[Fix[TaskConfigF]](appName)))
}
