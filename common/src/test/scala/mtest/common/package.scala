package mtest

import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickStatus}

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

package object common {
  def lazyTickList(init: TickStatus): LazyList[Tick] =
    LazyList.unfold(init)(ts => ts.next(ts.tick.wakeup.plus(1.milliseconds.toJava)).map(s => ((s.tick, s))))
}
