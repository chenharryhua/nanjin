package com.github.chenharryhua.nanjin.kafka.connector

import cats.data.ReaderT
import com.github.chenharryhua.nanjin.kafka.PartitionRange
import com.github.chenharryhua.nanjin.messages.kafka.CRMetaInfo
import fs2.Stream
import io.circe.syntax.EncoderOps
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

final case class ManualCommitStream[F[_], A](
  commitSync: ReaderT[F, Map[TopicPartition, OffsetAndMetadata], Unit],
  stream: Stream[F, A]
)

final case class RangedStream[F[_], A](
  stopConsuming: F[Unit], // help to stop the consumer
  streams: Map[PartitionRange, Stream[F, A]]
)

final case class PullDecodeException(meta: CRMetaInfo, cause: Throwable)
    extends Exception(meta.asJson.noSpaces, cause)
