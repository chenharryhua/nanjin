package com.github.chenharryhua.nanjin.kafka.record

import cats.Bitraverse
import cats.derived.derived
import io.circe.{Decoder, Encoder}

import java.time.ZonedDateTime

final case class ZonedConsumerRecord[K, V](
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: ZonedDateTime,
  timestampType: Int,
  headers: List[NJHeader],
  leaderEpoch: Option[Int],
  serializedKeySize: Int,
  serializedValueSize: Int,
  key: Option[K],
  value: Option[V]
) derives Encoder, Decoder, Bitraverse
