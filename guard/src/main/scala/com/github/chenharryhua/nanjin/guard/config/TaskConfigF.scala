package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.guard.TaskName
import eu.timepit.refined.cats.*
import higherkindness.droste.{scheme, Algebra}
import higherkindness.droste.data.Fix
import io.circe.generic.JsonCodec
import io.circe.refined.*
import monocle.macros.Lenses
import org.typelevel.cats.time.instances.zoneid

import java.time.ZoneId

@Lenses @JsonCodec final case class TaskParams(taskName: TaskName, zoneId: ZoneId, hostName: HostName)

object TaskParams extends zoneid {
  implicit val showTaskParams: Show[TaskParams] = cats.derived.semiauto.show[TaskParams]
}

sealed private[guard] trait TaskConfigF[X]

private object TaskConfigF {
  implicit val functorTaskConfigF: Functor[TaskConfigF] = cats.derived.semiauto.functor[TaskConfigF]

  final case class InitParams[K](taskName: TaskName, zoneId: ZoneId, hostName: HostName)
      extends TaskConfigF[K]
  final case class WithZoneId[K](value: ZoneId, cont: K) extends TaskConfigF[K]
  final case class WithHostName[K](value: HostName, cont: K) extends TaskConfigF[K]

  val algebra: Algebra[TaskConfigF, TaskParams] =
    Algebra[TaskConfigF, TaskParams] {
      case InitParams(taskName, hostName, zoneId) => TaskParams(taskName, hostName, zoneId)
      case WithZoneId(v, c)                       => TaskParams.zoneId.set(v)(c)
      case WithHostName(v, c)                     => TaskParams.hostName.set(v)(c)
    }
}

final case class TaskConfig private (value: Fix[TaskConfigF]) {
  import TaskConfigF.*

  def withZoneId(zoneId: ZoneId): TaskConfig       = TaskConfig(Fix(WithZoneId(zoneId, value)))
  def withHostName(hostName: HostName): TaskConfig = TaskConfig(Fix(WithHostName(hostName, value)))

  def evalConfig: TaskParams = scheme.cata(algebra).apply(value)
}

private[guard] object TaskConfig {

  def apply(taskName: TaskName, zoneId: ZoneId, hostName: HostName): TaskConfig = new TaskConfig(
    Fix(TaskConfigF.InitParams[Fix[TaskConfigF]](taskName, zoneId, hostName)))
}
