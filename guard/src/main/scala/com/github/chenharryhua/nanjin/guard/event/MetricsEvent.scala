package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.Codec

import java.time.ZonedDateTime

object MetricsEvent:

  sealed trait Index derives Codec.AsObject:
    def scrapeTime: ZonedDateTime

  object Index:
    final case class Adhoc(scrapeTime: ZonedDateTime) extends Index
    final case class Periodic(tick: Tick) extends Index:
      override val scrapeTime: ZonedDateTime = tick.zoned(_.conclude)

    given Show[Index]:
      override def show(t: Index): String = t match {
        case Index.Adhoc(_)       => "Adhoc"
        case Index.Periodic(tick) => tick.index.toString
      }
end MetricsEvent
