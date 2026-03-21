package com.github.chenharryhua.nanjin.kafka.record

import io.circe.Codec

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
) derives Codec.AsObject
