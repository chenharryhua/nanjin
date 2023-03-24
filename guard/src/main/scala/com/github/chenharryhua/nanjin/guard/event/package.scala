package com.github.chenharryhua.nanjin.guard
import cats.Show
import io.circe.{Decoder, Encoder, Json}
import squants.time.{Frequency, Hertz}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

package object event {
  implicit val hertzEncoder: Encoder[Frequency] = Encoder.instance(h => Json.fromDoubleOrNull(h.toHertz))
  implicit val hertzDecoder: Decoder[Frequency] = Decoder.decodeDouble.map(Hertz(_))
  implicit val showHertz: Show[Frequency]       = Show.fromToString

  implicit val encodeFiniteDuration: Encoder[FiniteDuration] = Encoder.encodeLong.contramap(_.toNanos)
  implicit val decodeFiniteDuration: Decoder[FiniteDuration] =
    Decoder.decodeLong.emap(nano => Right(FiniteDuration(nano, TimeUnit.NANOSECONDS)))
}
