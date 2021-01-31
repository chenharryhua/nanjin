package com.github.chenharryhua.nanjin.spark.kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.{stages, KafkaOffsetRange, KafkaTopic}
import com.github.chenharryhua.nanjin.spark.persist.{sinks, Compression}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import fs2.Stream
import fs2.interop.reactivestreams._
import io.circe.{Encoder => JsonEncoder}
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration.FiniteDuration

sealed class KafkaDownloader[F[_], K, V](
  akkaSystem: ActorSystem,
  topic: KafkaTopic[F, K, V],
  hadoop: Configuration,
  cfg: SKConfig) {
  val params: SKParams = cfg.evalConfig

  // config
  private def updateCfg(f: SKConfig => SKConfig): KafkaDownloader[F, K, V] =
    new KafkaDownloader[F, K, V](akkaSystem, topic, hadoop, f(cfg))

  def triggerEvery(ms: Long): KafkaDownloader[F, K, V]           = updateCfg(_.withLoadInterval(ms))
  def triggerEvery(ms: FiniteDuration): KafkaDownloader[F, K, V] = updateCfg(_.withLoadInterval(ms))
  def bulkSize(num: Int): KafkaDownloader[F, K, V]               = updateCfg(_.withLoadBulkSize(num))

  def recordsLimit(num: Long): KafkaDownloader[F, K, V]       = updateCfg(_.withLoadRecordsLimit(num))
  def timeLimit(ms: Long): KafkaDownloader[F, K, V]           = updateCfg(_.withLoadTimeLimit(ms))
  def timeLimit(fd: FiniteDuration): KafkaDownloader[F, K, V] = updateCfg(_.withLoadTimeLimit(fd))

  private def stream(implicit F: ConcurrentEffect[F], timer: Timer[F]): Stream[F, NJConsumerRecord[K, V]] = {
    val fstream: F[Stream[F, NJConsumerRecord[K, V]]] =
      topic.shortLiveConsumer.use(_.offsetRangeFor(params.timeRange).map(_.flatten[KafkaOffsetRange])).map { kor =>
        val src: Source[NJConsumerRecord[K, V], Consumer.Control] =
          if (kor.isEmpty)
            Source.empty.mapMaterializedValue(_ => Consumer.NoopControl)
          else
            topic
              .akkaChannel(akkaSystem)
              .assign(kor.value.mapValues(_.from.offset.value))
              .throttle(
                params.loadParams.bulkSize,
                params.loadParams.interval,
                cr => cr.serializedKeySize() + cr.serializedValueSize())
              .via(stages.takeUntilEnd(kor.mapValues(os => os.until.offset.value - 1)))
              .map(cr => NJConsumerRecord(topic.decoder(cr).optionalKeyValue))

        src
          .runWith(Sink.asPublisher(fanout = false))(Materializer(akkaSystem))
          .toStream
          .interruptAfter(params.loadParams.timeLimit)
          .take(params.loadParams.recordsLimit)
      }
    Stream.force(fstream)
  }

  def avro(path: String)(implicit F: ConcurrentEffect[F], timer: Timer[F]): AvroDownloader[F, K, V] = {
    val encoder: AvroEncoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef).avroEncoder
    new AvroDownloader(stream, encoder, hadoop, path, Compression.Uncompressed)
  }

  def circe(path: String)(implicit F: ConcurrentEffect[F], timer: Timer[F]): CirceDownloader[F, K, V] =
    new CirceDownloader[F, K, V](stream, hadoop, true, path, Compression.Uncompressed)
}

final class AvroDownloader[F[_], K, V](
  stream: Stream[F, NJConsumerRecord[K, V]],
  encoder: AvroEncoder[NJConsumerRecord[K, V]],
  hadoop: Configuration,
  path: String,
  compression: Compression) {

  private def updateCompression(compression: Compression): AvroDownloader[F, K, V] =
    new AvroDownloader[F, K, V](stream, encoder, hadoop, path, compression)

  def deflate(level: Int): AvroDownloader[F, K, V] = updateCompression(Compression.Deflate(level))
  def xz(level: Int): AvroDownloader[F, K, V]      = updateCompression(Compression.Xz(level))
  def snappy: AvroDownloader[F, K, V]              = updateCompression(Compression.Snappy)
  def bzip2: AvroDownloader[F, K, V]               = updateCompression(Compression.Bzip2)
  def uncompress: AvroDownloader[F, K, V]          = updateCompression(Compression.Uncompressed)

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): Stream[F, Unit] =
    stream.through(sinks.avro(path, hadoop, encoder, compression.avro(hadoop), blocker))
}

final class CirceDownloader[F[_], K, V](
  stream: Stream[F, NJConsumerRecord[K, V]],
  hadoop: Configuration,
  isKeepNull: Boolean,
  path: String,
  compression: Compression) {

  private def updateCompression(compression: Compression): CirceDownloader[F, K, V] =
    new CirceDownloader[F, K, V](stream, hadoop, isKeepNull, path, compression)

  def deflate(level: Int): CirceDownloader[F, K, V] = updateCompression(Compression.Deflate(level))
  def gzip: CirceDownloader[F, K, V]                = updateCompression(Compression.Gzip)
  def uncompress: CirceDownloader[F, K, V]          = updateCompression(Compression.Uncompressed)

  def keepNull: CirceDownloader[F, K, V] = new CirceDownloader[F, K, V](stream, hadoop, true, path, compression)
  def dropNull: CirceDownloader[F, K, V] = new CirceDownloader[F, K, V](stream, hadoop, false, path, compression)

  def run(blocker: Blocker)(implicit
    F: Sync[F],
    cs: ContextShift[F],
    enc: JsonEncoder[NJConsumerRecord[K, V]]): Stream[F, Unit] =
    stream.through(sinks.circe(path, hadoop, isKeepNull, compression.fs2Compression, blocker))
}
