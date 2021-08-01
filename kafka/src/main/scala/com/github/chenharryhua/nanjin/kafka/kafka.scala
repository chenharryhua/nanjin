package com.github.chenharryhua.nanjin

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
package object kafka {

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
