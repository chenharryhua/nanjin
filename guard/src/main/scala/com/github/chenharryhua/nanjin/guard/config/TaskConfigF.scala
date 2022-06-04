package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.guard.{HomePage, TaskName}
import eu.timepit.refined.cats.*
import higherkindness.droste.{scheme, Algebra}
import higherkindness.droste.data.Fix
import io.circe.generic.JsonCodec
import io.circe.refined.*
import monocle.macros.Lenses
import org.typelevel.cats.time.instances.zoneid

import java.time.ZoneId

@Lenses @JsonCodec final case class TaskParams private (
  taskName: TaskName,
  zoneId: ZoneId,
  hostName: HostName,
  homePage: Option[HomePage])

private[guard] object TaskParams extends zoneid {
  implicit val showTaskParams: Show[TaskParams] = cats.derived.semiauto.show[TaskParams]

  def apply(taskName: TaskName, hostName: HostName): TaskParams = TaskParams(
    taskName = taskName,
    zoneId = ZoneId.systemDefault(),
    hostName = hostName,
    homePage = None
  )
}

sealed private[guard] trait TaskConfigF[X]

private object TaskConfigF {
  implicit val functorTaskConfigF: Functor[TaskConfigF] = cats.derived.semiauto.functor[TaskConfigF]

  final case class InitParams[K](taskName: TaskName, hostName: HostName) extends TaskConfigF[K]
  final case class WithZoneId[K](value: ZoneId, cont: K) extends TaskConfigF[K]
  final case class WithHostName[K](value: HostName, cont: K) extends TaskConfigF[K]
  final case class WithHomePage[K](value: Option[HomePage], cont: K) extends TaskConfigF[K]

  val algebra: Algebra[TaskConfigF, TaskParams] =
    Algebra[TaskConfigF, TaskParams] {
      case InitParams(taskName, hostName) => TaskParams(taskName, hostName)
      case WithZoneId(v, c)               => TaskParams.zoneId.set(v)(c)
      case WithHostName(v, c)             => TaskParams.hostName.set(v)(c)
      case WithHomePage(v, c)             => TaskParams.homePage.set(v)(c)
    }
}

final case class TaskConfig private (value: Fix[TaskConfigF]) {
  import TaskConfigF.*

  def withZoneId(zoneId: ZoneId): TaskConfig       = TaskConfig(Fix(WithZoneId(zoneId, value)))
  def withHostName(hostName: HostName): TaskConfig = TaskConfig(Fix(WithHostName(hostName, value)))
  def withHomePage(url: HomePage): TaskConfig      = TaskConfig(Fix(WithHomePage(Some(url), value)))

  def evalConfig: TaskParams = scheme.cata(algebra).apply(value)
}

private[guard] object TaskConfig {

  def apply(taskName: TaskName, hostName: HostName): TaskConfig = new TaskConfig(
    Fix(TaskConfigF.InitParams[Fix[TaskConfigF]](taskName, hostName)))
}
