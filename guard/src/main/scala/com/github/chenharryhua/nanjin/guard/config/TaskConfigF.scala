package com.github.chenharryhua.nanjin.guard.config

import cats.Functor
import com.github.chenharryhua.nanjin.common.HostName
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import monocle.syntax.all.*

import java.time.ZoneId

@JsonCodec
final case class TaskParams(taskName: String, zoneId: ZoneId, hostName: HostName, homePage: Option[String])

sealed private[guard] trait TaskConfigF[X]

private object TaskConfigF {
  implicit val functorTaskConfigF: Functor[TaskConfigF] = cats.derived.semiauto.functor[TaskConfigF]

  final case class InitParams[K](taskName: String) extends TaskConfigF[K]
  final case class WithZoneId[K](value: ZoneId, cont: K) extends TaskConfigF[K]
  final case class WithHostName[K](value: HostName, cont: K) extends TaskConfigF[K]
  final case class WithHomePage[K](value: Option[String], cont: K) extends TaskConfigF[K]

  val algebra: Algebra[TaskConfigF, TaskParams] =
    Algebra[TaskConfigF, TaskParams] {
      case InitParams(taskName) => TaskParams(taskName, ZoneId.systemDefault(), HostName.local_host, None)
      case WithZoneId(v, c)     => c.focus(_.zoneId).replace(v)
      case WithHostName(v, c)   => c.focus(_.hostName).replace(v)
      case WithHomePage(v, c)   => c.focus(_.homePage).replace(v)
    }
}

final case class TaskConfig(cont: Fix[TaskConfigF]) {
  import TaskConfigF.*

  def withZoneId(zoneId: ZoneId): TaskConfig       = TaskConfig(Fix(WithZoneId(zoneId, cont)))
  def withHostName(hostName: HostName): TaskConfig = TaskConfig(Fix(WithHostName(hostName, cont)))
  def withHomePage(hp: String): TaskConfig         = TaskConfig(Fix(WithHomePage(Some(hp), cont)))

  def evalConfig: TaskParams = scheme.cata(algebra).apply(cont)
}

object TaskConfig {

  def apply(taskName: String): TaskConfig =
    new TaskConfig(Fix(TaskConfigF.InitParams[Fix[TaskConfigF]](taskName)))
}
