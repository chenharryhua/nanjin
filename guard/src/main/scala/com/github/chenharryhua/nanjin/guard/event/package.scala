package com.github.chenharryhua.nanjin.guard
import io.circe.{Decoder, Encoder, Json}
import squants.time.{Frequency, Hertz}

package object event {
  implicit val hertzEncoder: Encoder[Frequency] = Encoder.instance(h => Json.fromDoubleOrNull(h.toHertz))
  implicit val hertzDecoder: Decoder[Frequency] = Decoder.decodeDouble.map(Hertz(_))
}
