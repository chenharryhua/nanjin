package com.github.chenharryhua.nanjin.spark.kafka

import akka.actor.ActorSystem
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.persist.{sinks, Compression}
import fs2.Stream
import org.apache.hadoop.conf.Configuration

sealed class AkkaDownloader[F[_], K, V](
  akkaSystem: ActorSystem,
  topic: KafkaTopic[F, K, V],
  cfg: SKConfig,
  hadoop: Configuration) {
  val params: SKParams = cfg.evalConfig

  protected def stream(implicit F: ConcurrentEffect[F], timer: Timer[F]): Stream[F, NJConsumerRecord[K, V]] =
    topic
      .akkaChannel(akkaSystem)
      .timeRanged(params.timeRange)
      .map(cr => NJConsumerRecord(topic.decoder(cr).optionalKeyValue))
      .chunkN(params.uploadParams.bufferSize)
      .metered(params.uploadParams.interval)
      .take(params.uploadParams.recordsLimit)
      .interruptAfter(params.uploadParams.timeLimit)
      .flatMap(Stream.chunk)

  def avro(path: String): AvroDownloader[F, K, V] =
    new AvroDownloader(akkaSystem, topic, cfg, hadoop, path, Compression.Uncompressed)

}

final class AvroDownloader[F[_], K, V](
  akkaSystem: ActorSystem,
  topic: KafkaTopic[F, K, V],
  cfg: SKConfig,
  hadoop: Configuration,
  path: String,
  compression: Compression
) extends AkkaDownloader[F, K, V](akkaSystem, topic, cfg, hadoop) {

  def run(blocker: Blocker)(implicit F: ConcurrentEffect[F], timer: Timer[F], cs: ContextShift[F]): Stream[F, Unit] = {
    val encoder = NJConsumerRecord.avroCodec(topic.topicDef).avroEncoder
    stream.through(sinks.avro(path, hadoop, encoder, compression.avro(hadoop), blocker))
  }
}
