package com.github.chenharryhua.nanjin.terminals

import cats.Show
import cats.syntax.eq.given
import io.circe.{Codec, Decoder, Encoder}
import io.lemonlabs.uri.Url

enum RetentionStatus:
  case Removed, RemovalFailed, Retained

object RetentionStatus:
  given Show[RetentionStatus] = _.productPrefix
  given Encoder[RetentionStatus] = Encoder.encodeString.contramap(_.productPrefix)
  given Decoder[RetentionStatus] = Decoder[String].emap { s =>
    RetentionStatus.values.find(_.productPrefix === s).toRight(s"Invalid RetentionStatus: $s")
  }

final case class FolderRetentionStatus(folder: Url, status: RetentionStatus) derives Codec.AsObject
