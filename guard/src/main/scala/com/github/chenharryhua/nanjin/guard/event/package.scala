package com.github.chenharryhua.nanjin.guard

import cats.Show
import io.circe.{Decoder, Encoder}
import io.scalaland.enumz.Enum

import java.util.concurrent.TimeUnit

package object event {
  private[this] val enumTimeUnit: Enum[TimeUnit] = Enum[TimeUnit]

  implicit private[event] val encoderTimeUnit: Encoder[TimeUnit] = Encoder.encodeString.contramap(enumTimeUnit.getName)
  implicit private[event] val decoderTimeUnit: Decoder[TimeUnit] = Decoder.decodeString.map(enumTimeUnit.withName)
  implicit private[event] val showTimeUnit: Show[TimeUnit]       = enumTimeUnit.getName
}
