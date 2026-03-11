package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import io.circe.Codec

import java.time.ZonedDateTime

object MetricsEvent {

  sealed trait Index extends Product derives Codec.AsObject {
    def scrapeTime: ZonedDateTime
  }

  object Index {
    final case class Adhoc(scrapeTime: ZonedDateTime) extends Index
    final case class Periodic(tick: Tick) extends Index {
      override val scrapeTime: ZonedDateTime = tick.zoned(_.conclude)
    }
  }

  sealed trait Kind extends Product derives Codec.AsObject {
    def policy: Policy
    final override def toString: String = this.productPrefix
  }
  object Kind {
    final case class Report(policy: Policy) extends Kind
    final case class Reset(policy: Policy) extends Kind

    given Show[Kind] = Show.fromToString
  }
}
