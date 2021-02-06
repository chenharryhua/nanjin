package com.github.chenharryhua.nanjin

import eu.timepit.refined.W
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.string.MatchesRegex

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
package object kafka {

  type TopicName = String Refined MatchesRegex[W.`"^[a-zA-Z0-9_.-]+$"`.T]

  object TopicName extends RefinedTypeOps[TopicName, String] with CatsRefinedTypeOpsSyntax

  type StoreName = String Refined MatchesRegex[W.`"^[a-zA-Z0-9_.-]+$"`.T]

  object StoreName extends RefinedTypeOps[StoreName, String] with CatsRefinedTypeOpsSyntax

  object defaultLoadParams {
    //akka.actor.LightArrayRevolverScheduler.checkMaxDelay
    final val TimeLimit: FiniteDuration   = FiniteDuration(21474835, TimeUnit.SECONDS)
    final val RecordsLimit: Long          = Long.MaxValue
    final val IdleTimeout: FiniteDuration = FiniteDuration(120, TimeUnit.SECONDS)
    final val BufferSize: Int             = 15
    final val Interval: FiniteDuration    = FiniteDuration(1, TimeUnit.SECONDS)
    final val BulkSize: Int               = 1024 * 1024
    final val BatchSize: Int              = 1000
  }
}
