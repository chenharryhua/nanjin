package com.github.chenharryhua.nanjin.common.logging

import com.github.chenharryhua.nanjin.common.OpaqueLift
import io.circe.{Decoder, Encoder}

import java.time.Instant

opaque type LogLink = String
object LogLink:
  def apply(str: String): LogLink = str

  extension (ll: LogLink) inline def value: String = ll

  given Encoder[LogLink] = OpaqueLift.lift[LogLink, String, Encoder]
  given Decoder[LogLink] = OpaqueLift.lift[LogLink, String, Decoder]
end LogLink

trait LogLocator {
  def locate(timestamp: Instant): LogLink
}
