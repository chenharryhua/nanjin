package com.github.chenharryhua.nanjin.http.client.auth

import cats.syntax.either.given
import io.circe.{Decoder, Encoder}
import org.http4s.Uri

object UriJsonCodec {
  given Encoder[Uri] = Encoder.encodeString.contramap { uri =>
    uri.renderString
  }

  given Decoder[Uri] = Decoder.decodeString.emap { value =>
    Uri.fromString(value).leftMap(_.message)
  }
}
