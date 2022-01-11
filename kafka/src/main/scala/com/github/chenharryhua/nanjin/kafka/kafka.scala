package com.github.chenharryhua.nanjin

import com.github.chenharryhua.nanjin.common.ChunkSize

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
package object kafka {

  object defaultLoadParams {
    // akka.actor.LightArrayRevolverScheduler.checkMaxDelay
    final val TimeLimit: FiniteDuration   = FiniteDuration(21474835, TimeUnit.SECONDS)
    final val RecordsLimit: Long          = Long.MaxValue
    final val IdleTimeout: FiniteDuration = FiniteDuration(120, TimeUnit.SECONDS)
    final val bufferSize: Int             = 16
    final val Interval: FiniteDuration    = FiniteDuration(1, TimeUnit.SECONDS)
    final val bulkSize: Int               = 1024 * 1024
    final val chunkSize: ChunkSize        = ChunkSize(1000)
  }
}
