package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.generic.JsonCodec

import java.time.ZonedDateTime

object MetricsReportData {
  @JsonCodec
  sealed trait Index extends Product {
    def launchTime: ZonedDateTime
  }

  object Index {
    final case class Adhoc(value: ZonedDateTime) extends Index {
      override val launchTime: ZonedDateTime = value
    }

    final case class Periodic(tick: Tick) extends Index {
      override val launchTime: ZonedDateTime = tick.zoned(_.conclude)
    }

    implicit val showMetricsIndex: Show[Index] = {
      case Adhoc(_)       => "Adhoc"
      case Periodic(tick) => tick.index.toString
    }
  }
}
