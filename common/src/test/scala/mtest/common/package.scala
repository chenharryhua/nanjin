package mtest

import com.github.chenharryhua.nanjin.common.chrono.zones.beijingTime
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickStatus}

import java.time.{Duration, ZonedDateTime}
import java.util.UUID

package object common {

  private val today: ZonedDateTime = ZonedDateTime.of(2050, 1, 1, 0, 0, 0, 0, beijingTime)

  val zeroTickStatus: TickStatus = TickStatus(
    Tick(
      sequenceId = UUID.randomUUID(),
      launchTime = today.toInstant,
      zoneId = beijingTime,
      previous = today.toInstant,
      index = 0,
      acquire = today.toInstant,
      snooze = Duration.ZERO
    ))
}
