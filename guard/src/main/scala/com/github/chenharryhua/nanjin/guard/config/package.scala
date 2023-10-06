package com.github.chenharryhua.nanjin.guard

import cats.Show
import io.circe.{Decoder, Encoder}
import io.scalaland.enumz.Enum

import java.util.concurrent.TimeUnit

package object config {

  private val enumTimeUnit: Enum[TimeUnit]              = Enum[TimeUnit]
  implicit final val encoderTimeUnit: Encoder[TimeUnit] = Encoder.encodeString.contramap(enumTimeUnit.getName)
  implicit final val decoderTimeUnit: Decoder[TimeUnit] = Decoder.decodeString.map(enumTimeUnit.withName)
  implicit final val showTimeUnit: Show[TimeUnit]       = enumTimeUnit.getName(_)
}
