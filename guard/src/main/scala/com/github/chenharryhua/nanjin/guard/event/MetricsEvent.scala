package com.github.chenharryhua.nanjin.guard.event

import cats.Show
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.Timestamp
import io.circe.Codec

object MetricsEvent:

  sealed trait Index derives Codec.AsObject:
    def scrapeTime: Timestamp

  object Index:
    final case class Adhoc(scrapeTime: Timestamp) extends Index
    final case class Periodic(tick: Tick) extends Index:
      override val scrapeTime: Timestamp = Timestamp(tick.zoned(_.conclude))

    given Show[Index]:
      override def show(t: Index): String = t match {
        case Index.Adhoc(_)       => "Adhoc"
        case Index.Periodic(tick) => tick.index.toString
      }
end MetricsEvent
