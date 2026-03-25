package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import io.circe.Codec

import java.time.ZonedDateTime

object MetricsEvent:

  sealed trait Index derives Codec.AsObject:
    def scrapeTime: ZonedDateTime

  object Index:
    final case class Adhoc(scrapeTime: ZonedDateTime) extends Index
    final case class Periodic(tick: Tick) extends Index:
      override val scrapeTime: ZonedDateTime = tick.zoned(_.conclude)

  enum Kind derives Codec.AsObject:
    def policy: Policy

    case Report(policy: Policy)
    case Reset(policy: Policy)

  object Kind:
    given Show[Kind] = _.productPrefix

end MetricsEvent
