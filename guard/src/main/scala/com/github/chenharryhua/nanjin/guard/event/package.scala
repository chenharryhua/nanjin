package com.github.chenharryhua.nanjin.guard
import cats.Show
import io.circe.{Decoder, Encoder, Json}
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit
import squants.time.{Frequency, Hertz}
import io.scalaland.enumz.Enum
package object event {
  implicit val hertzEncoder: Encoder[Frequency] = Encoder.instance(h => Json.fromDoubleOrNull(h.toHertz))
  implicit val hertzDecoder: Decoder[Frequency] = Decoder.decodeDouble.map(Hertz(_))
  implicit val showHertz: Show[Frequency]       = Show.fromToString

  private val esu: Enum[StandardUnit] = Enum[StandardUnit]
  implicit val standardUnitEncoder: Encoder[StandardUnit] =
    Encoder.instance(su => Json.fromString(esu.getName(su)))

  implicit val standardUnitDecoder: Decoder[StandardUnit] =
    Decoder.decodeString.emap(u =>
      esu.withNameOption(u) match {
        case Some(value) => Right(value)
        case None        => Left(s"$u is an invalid CloudWatch StandardUnit")
      })

  implicit val showStandardUnit: Show[StandardUnit] = esu.getName(_).toLowerCase()
}
