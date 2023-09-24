package mtest

import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickStatus}

import java.time.Instant

package object common {
  def lazyTickList(init: TickStatus): LazyList[Tick] =
    LazyList.unfold(init)(ts => ts.next(Instant.now()).map(s => ((s.tick, s))))
}
