package com.github.chenharryhua.nanjin.guard

import cats.Show
import io.circe.{Decoder, Encoder, Json}
import io.scalaland.enumz.Enum
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

import java.util.concurrent.TimeUnit

package object config {

  private val esu: Enum[StandardUnit] = Enum[StandardUnit]
  implicit val standardUnitEncoder: Encoder[StandardUnit] =
    Encoder.instance(su => Json.fromString(esu.getName(su)))

  implicit val standardUnitDecoder: Decoder[StandardUnit] =
    Decoder.decodeString.emap(ut =>
      esu.withNameOption(ut) match {
        case Some(value) => Right(value)
        case None        => Left(s"$ut is an invalid CloudWatch StandardUnit")
      })

  implicit val showStandardUnit: Show[StandardUnit] = esu.getName(_).toLowerCase()

  private val enumTimeUnit: Enum[TimeUnit]              = Enum[TimeUnit]
  implicit final val encoderTimeUnit: Encoder[TimeUnit] = Encoder.encodeString.contramap(enumTimeUnit.getName)
  implicit final val decoderTimeUnit: Decoder[TimeUnit] = Decoder.decodeString.map(enumTimeUnit.withName)
  implicit final val showTimeUnit: Show[TimeUnit]       = enumTimeUnit.getName(_)
}
