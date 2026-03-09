package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import io.circe.Codec

import java.time.ZonedDateTime

object MetricsEvent {

  sealed trait Index extends Product derives Codec.AsObject {
    def launchTime: ZonedDateTime
  }

  object Index {
    final case class Adhoc(launchTime: ZonedDateTime) extends Index
    final case class Periodic(tick: Tick) extends Index {
      override val launchTime: ZonedDateTime = tick.zoned(_.conclude)
    }
  }

  sealed trait Kind extends Product derives Codec.AsObject {
    def policy: Policy
    final override def toString: String = this.productPrefix
  }
  object Kind {
    final case class Report(policy: Policy) extends Kind
    final case class Reset(policy: Policy) extends Kind

    implicit val showMetricsKind: Show[Kind] = Show.fromToString
  }
}
