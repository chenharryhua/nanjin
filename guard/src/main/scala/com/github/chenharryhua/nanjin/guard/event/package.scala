package com.github.chenharryhua.nanjin.guard
import cats.Show
import io.circe.{Decoder, Encoder, Json}

import squants.time.{Frequency, Hertz}

package object event {
  implicit val hertzEncoder: Encoder[Frequency] = Encoder.instance(h => Json.fromDoubleOrNull(h.toHertz))
  implicit val hertzDecoder: Decoder[Frequency] = Decoder.decodeDouble.map(Hertz(_))
  implicit val showHertz: Show[Frequency]       = Show.fromToString

}
