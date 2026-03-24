package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import io.circe.Codec

import java.time.ZonedDateTime

object MetricsEvent:

  enum Index derives Codec.AsObject:
    def scrapeTime: ZonedDateTime =
      this match
        case Adhoc(time)    => time
        case Periodic(tick) => tick.zoned(_.conclude)

    case Adhoc(time: ZonedDateTime)
    case Periodic(tick: Tick)
  end Index

  enum Kind derives Codec.AsObject:
    def policy: Policy

    case Report(policy: Policy)
    case Reset(policy: Policy)

  object Kind:
    given Show[Kind] = _.productPrefix
    
end MetricsEvent
