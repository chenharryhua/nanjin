package example

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{policies, tickStream, Policy, Tick}
import monocle.Monocle.toAppliedFocusOps
import fs2.Stream
import java.time.YearMonth

object last_day_of_month {

  val policy: Policy = policies.crontab(_.daily.oneAM)

  val ts: Stream[IO, Tick] = tickStream[IO](policy, sydneyTime)
    .filter(tick => YearMonth.from(tick.zonedWakeup).atEndOfMonth() == tick.zonedWakeup.toLocalDate)
    .zipWithIndex
    .map { case (tick, idx) => tick.focus(_.index).replace(idx + 1) }

}
