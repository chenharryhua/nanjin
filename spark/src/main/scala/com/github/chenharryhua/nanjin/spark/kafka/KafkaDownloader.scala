package com.github.chenharryhua.nanjin.spark.kafka

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.kafka.{akkaUpdater, stages, KafkaTopic}
import com.github.chenharryhua.nanjin.spark.persist.{sinks, Compression}
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.Stream
import fs2.interop.reactivestreams.*
import io.circe.Encoder as JsonEncoder
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration.FiniteDuration

/** Notes
  *
  * the downloader is able to control download rate from Kafka
  *
  * the maximum rate is about <b>BulkSize</b> per <b>Interval</b>
  *
  * [[circe]] is not isomorphic when key or value of ConsumerRecord is a coproduct
  */
final class KafkaDownloader[F[_], K, V](
  akkaSystem: ActorSystem,
  topic: KafkaTopic[F, K, V],
  hadoop: Configuration,
  cfg: SKConfig,
  akkaConsumer: akkaUpdater.Consumer) {
  val params: SKParams = cfg.evalConfig

  def updateConsumer(f: ConsumerSettings[Array[Byte], Array[Byte]] => ConsumerSettings[Array[Byte], Array[Byte]])
    : KafkaDownloader[F, K, V] =
    new KafkaDownloader[F, K, V](akkaSystem, topic, hadoop, cfg, akkaConsumer.updateConfig(f))

  // config
  private def updateCfg(f: SKConfig => SKConfig): KafkaDownloader[F, K, V] =
    new KafkaDownloader[F, K, V](akkaSystem, topic, hadoop, f(cfg), akkaConsumer)

  def withInterval(fd: FiniteDuration): KafkaDownloader[F, K, V] = updateCfg(_.loadInterval(fd))
  def withBulkSize(num: Int): KafkaDownloader[F, K, V]           = updateCfg(_.loadBulkSize(num))

  def withRecordsLimit(num: Long): KafkaDownloader[F, K, V]       = updateCfg(_.loadRecordsLimit(num))
  def withTimeLimit(fd: FiniteDuration): KafkaDownloader[F, K, V] = updateCfg(_.loadTimeLimit(fd))

  def withIdleTimeout(fd: FiniteDuration): KafkaDownloader[F, K, V] = updateCfg(_.loadIdleTimeout(fd))

  private def stream(implicit F: Async[F]): Stream[F, NJConsumerRecord[K, V]] = {
    val fstream: F[Stream[F, NJConsumerRecord[K, V]]] =
      topic.shortLiveConsumer.use(_.offsetRangeFor(params.timeRange).map(_.flatten)).map { kor =>
        topic
          .akkaChannel(akkaSystem)
          .updateConsumer(akkaConsumer.updates.run)
          .assign(kor.mapValues(_.from))
          .throttle(
            params.loadParams.bulkSize,
            params.loadParams.interval,
            cr => cr.serializedKeySize() + cr.serializedValueSize())
          .via(stages.takeUntilEnd(kor.mapValues(_.until)))
          .map(cr => NJConsumerRecord(topic.decoder(cr).optionalKeyValue))
          .idleTimeout(params.loadParams.idleTimeout)
          .runWith(Sink.asPublisher(fanout = false))(Materializer(akkaSystem))
          .toStreamBuffered(params.loadParams.bufferSize)
          .interruptAfter(params.loadParams.timeLimit)
          .take(params.loadParams.recordsLimit)
      }
    Stream.force(fstream)
  }

  def avro(path: String)(implicit F: Async[F]): AvroDownloader[F, K, V] = {
    val encoder: AvroEncoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef).avroEncoder
    new AvroDownloader(stream, encoder, hadoop, path, Compression.Uncompressed)
  }

  def jackson(path: String)(implicit F: Async[F]): JacksonDownloader[F, K, V] = {
    val encoder: AvroEncoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef).avroEncoder
    new JacksonDownloader(stream, encoder, hadoop, path, Compression.Uncompressed)
  }

  def circe(path: String)(implicit F: Async[F]): CirceDownloader[F, K, V] =
    new CirceDownloader[F, K, V](stream, hadoop, path, true, Compression.Uncompressed)

  def parquet(path: String)(implicit F: Async[F]): ParquetDownloader[F, K, V] = {
    val encoder: AvroEncoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef).avroEncoder
    new ParquetDownloader(stream, encoder, hadoop, path, Compression.Uncompressed)
  }
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

  def run(implicit F: Sync[F]): Stream[F, Unit] =
    stream.through(sinks.avro(path, hadoop, encoder, compression.avro(hadoop)))
}

final class JacksonDownloader[F[_], K, V](
  stream: Stream[F, NJConsumerRecord[K, V]],
  encoder: AvroEncoder[NJConsumerRecord[K, V]],
  hadoop: Configuration,
  path: String,
  compression: Compression) {

  private def updateCompression(compression: Compression): JacksonDownloader[F, K, V] =
    new JacksonDownloader[F, K, V](stream, encoder, hadoop, path, compression)

  def deflate(level: Int): JacksonDownloader[F, K, V] = updateCompression(Compression.Deflate(level))
  def gzip: JacksonDownloader[F, K, V]                = updateCompression(Compression.Gzip)
  def uncompress: JacksonDownloader[F, K, V]          = updateCompression(Compression.Uncompressed)

  def run(implicit F: Sync[F]): Stream[F, Unit] =
    stream.through(sinks.jackson(path, hadoop, encoder, compression.fs2Compression))
}

final class CirceDownloader[F[_], K, V](
  stream: Stream[F, NJConsumerRecord[K, V]],
  hadoop: Configuration,
  path: String,
  isKeepNull: Boolean,
  compression: Compression) {

  private def updateCompression(compression: Compression): CirceDownloader[F, K, V] =
    new CirceDownloader[F, K, V](stream, hadoop, path, isKeepNull, compression)

  def deflate(level: Int): CirceDownloader[F, K, V] = updateCompression(Compression.Deflate(level))
  def gzip: CirceDownloader[F, K, V]                = updateCompression(Compression.Gzip)
  def uncompress: CirceDownloader[F, K, V]          = updateCompression(Compression.Uncompressed)

  def keepNull: CirceDownloader[F, K, V] = new CirceDownloader[F, K, V](stream, hadoop, path, true, compression)
  def dropNull: CirceDownloader[F, K, V] = new CirceDownloader[F, K, V](stream, hadoop, path, false, compression)

  def run(implicit F: Sync[F], enc: JsonEncoder[NJConsumerRecord[K, V]]): Stream[F, Unit] =
    stream.through(sinks.circe(path, hadoop, isKeepNull, compression.fs2Compression))
}

final class ParquetDownloader[F[_], K, V](
  stream: Stream[F, NJConsumerRecord[K, V]],
  encoder: AvroEncoder[NJConsumerRecord[K, V]],
  hadoop: Configuration,
  path: String,
  compression: Compression) {

  private def updateCompression(compression: Compression): ParquetDownloader[F, K, V] =
    new ParquetDownloader[F, K, V](stream, encoder, hadoop, path, compression)

  def snappy: ParquetDownloader[F, K, V]     = updateCompression(Compression.Snappy)
  def gzip: ParquetDownloader[F, K, V]       = updateCompression(Compression.Gzip)
  def uncompress: ParquetDownloader[F, K, V] = updateCompression(Compression.Uncompressed)

  def run(implicit F: Sync[F]): Stream[F, Unit] =
    stream.through(sinks.parquet(path, hadoop, encoder, compression.parquet))
}
