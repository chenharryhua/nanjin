package com.github.chenharryhua.nanjin.guard
import cats.Show
import io.circe.{Decoder, Encoder, HCursor, Json}
import squants.time.{Frequency, Hertz}

package object event {
  implicit val hertzEncoder: Encoder[Frequency] = (a: Frequency) => Json.fromDoubleOrNull(a.toHertz)
  implicit val hertzDecoder: Decoder[Frequency] = (c: HCursor) => c.as[Double].map(Hertz(_))
  implicit val showHertz: Show[Frequency]       = Show.fromToString
}
