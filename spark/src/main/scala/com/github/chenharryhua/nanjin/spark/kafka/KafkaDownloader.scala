package com.github.chenharryhua.nanjin.spark.kafka

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import cats.data.Kleisli
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.kafka.{akkaUpdater, stages, KafkaTopic}
import com.github.chenharryhua.nanjin.spark.persist.{sinks, Compression}
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.interop.reactivestreams.*
import fs2.{Pipe, Stream}
import io.circe.Encoder as JsonEncoder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import squants.information.Information

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
  akkaConsumer: akkaUpdater.Consumer,
  listener: Option[Kleisli[F, NJConsumerRecord[K, V], Unit]]) {

  def updateConsumer(f: ConsumerSettings[Array[Byte], Array[Byte]] => ConsumerSettings[Array[Byte], Array[Byte]])
    : KafkaDownloader[F, K, V] =
    new KafkaDownloader[F, K, V](akkaSystem, topic, hadoop, cfg, akkaConsumer.updateConfig(f), listener)

  // config
  private def updateCfg(f: SKConfig => SKConfig): KafkaDownloader[F, K, V] =
    new KafkaDownloader[F, K, V](akkaSystem, topic, hadoop, f(cfg), akkaConsumer, listener)

  def withInterval(fd: FiniteDuration): KafkaDownloader[F, K, V] = updateCfg(_.loadInterval(fd))
  def withThrottle(num: Information): KafkaDownloader[F, K, V]   = updateCfg(_.loadThrottle(num))

  def withRecordsLimit(num: Long): KafkaDownloader[F, K, V]       = updateCfg(_.loadRecordsLimit(num))
  def withTimeLimit(fd: FiniteDuration): KafkaDownloader[F, K, V] = updateCfg(_.loadTimeLimit(fd))

  def withChunkSize(cs: ChunkSize): KafkaDownloader[F, K, V] = updateCfg(_.loadChunkSize(cs))

  def withIdleTimeout(fd: FiniteDuration): KafkaDownloader[F, K, V] = updateCfg(_.loadIdleTimeout(fd))

  def withListener(f: NJConsumerRecord[K, V] => F[Unit]): KafkaDownloader[F, K, V] =
    new KafkaDownloader[F, K, V](akkaSystem, topic, hadoop, cfg, akkaConsumer, Some(Kleisli(f)))

  private def stream(implicit F: Async[F]): Stream[F, NJConsumerRecord[K, V]] = {
    val params: SKParams = cfg.evalConfig
    val fstream: F[Stream[F, NJConsumerRecord[K, V]]] =
      topic.shortLiveConsumer.use(_.offsetRangeFor(params.timeRange).map(_.flatten)).map { kor =>
        topic
          .akkaChannel(akkaSystem)
          .updateConsumer(akkaConsumer.updates.run)
          .assign(kor.mapValues(_.from))
          .throttle(
            params.loadParams.throttle.toBytes.toInt,
            params.loadParams.interval,
            cr => cr.serializedKeySize() + cr.serializedValueSize())
          .via(stages.takeUntilEnd(kor.mapValues(_.until)))
          .map(cr => NJConsumerRecord(topic.decoder(cr).optionalKeyValue))
          .idleTimeout(params.loadParams.idleTimeout)
          .runWith(Sink.asPublisher(fanout = false))(Materializer(akkaSystem))
          .toStreamBuffered(params.loadParams.chunkSize.value)
          .interruptAfter(params.loadParams.timeLimit)
          .take(params.loadParams.recordsLimit)
      }
    Stream.force(fstream)
  }

  def avro(path: NJPath)(implicit F: Async[F]): AvroDownloader[F, K, V] = {
    val encoder: AvroEncoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef).avroEncoder
    new AvroDownloader(stream, encoder, hadoop, path, Compression.Uncompressed, listener)
  }

  def jackson(path: NJPath)(implicit F: Async[F]): JacksonDownloader[F, K, V] = {
    val encoder: AvroEncoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef).avroEncoder
    new JacksonDownloader(stream, encoder, hadoop, path, Compression.Uncompressed, listener)
  }

  def circe(path: NJPath)(implicit F: Async[F]): CirceDownloader[F, K, V] =
    new CirceDownloader[F, K, V](stream, hadoop, path, isKeepNull = true, Compression.Uncompressed, listener)

  def parquet(path: NJPath)(implicit F: Async[F]): ParquetDownloader[F, K, V] = {
    val encoder: AvroEncoder[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef).avroEncoder

    val builder: AvroParquetWriter.Builder[GenericRecord] = AvroParquetWriter
      .builder[GenericRecord](HadoopOutputFile.fromPath(path.hadoopPath, hadoop))
      .withDataModel(GenericData.get())
      .withSchema(encoder.schema)
      .withConf(hadoop)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)

    new ParquetDownloader(stream, encoder, builder, listener)
  }
}

final class AvroDownloader[F[_], K, V] private[kafka] (
  stream: Stream[F, NJConsumerRecord[K, V]],
  encoder: AvroEncoder[NJConsumerRecord[K, V]],
  hadoop: Configuration,
  path: NJPath,
  compression: Compression,
  listener: Option[Kleisli[F, NJConsumerRecord[K, V], Unit]]) {

  private def updateCompression(compression: Compression): AvroDownloader[F, K, V] =
    new AvroDownloader[F, K, V](stream, encoder, hadoop, path, compression, listener)

  def deflate(level: Int): AvroDownloader[F, K, V] = updateCompression(Compression.Deflate(level))
  def xz(level: Int): AvroDownloader[F, K, V]      = updateCompression(Compression.Xz(level))
  def snappy: AvroDownloader[F, K, V]              = updateCompression(Compression.Snappy)
  def bzip2: AvroDownloader[F, K, V]               = updateCompression(Compression.Bzip2)
  def uncompress: AvroDownloader[F, K, V]          = updateCompression(Compression.Uncompressed)

  def sink(implicit F: Sync[F]): Stream[F, Unit] = {
    val tgt: Pipe[F, NJConsumerRecord[K, V], Unit] = sinks.avro(path, hadoop, encoder, compression.avro(hadoop))
    listener.fold(stream.through(tgt))(k => stream.evalTap(k.run).through(tgt))
  }
}

final class JacksonDownloader[F[_], K, V] private[kafka] (
  stream: Stream[F, NJConsumerRecord[K, V]],
  encoder: AvroEncoder[NJConsumerRecord[K, V]],
  hadoop: Configuration,
  path: NJPath,
  compression: Compression,
  listener: Option[Kleisli[F, NJConsumerRecord[K, V], Unit]]) {

  private def updateCompression(compression: Compression): JacksonDownloader[F, K, V] =
    new JacksonDownloader[F, K, V](stream, encoder, hadoop, path, compression, listener)

  def deflate(level: Int): JacksonDownloader[F, K, V] = updateCompression(Compression.Deflate(level))
  def gzip: JacksonDownloader[F, K, V]                = updateCompression(Compression.Gzip)
  def uncompress: JacksonDownloader[F, K, V]          = updateCompression(Compression.Uncompressed)

  def sink(implicit F: Sync[F]): Stream[F, Unit] = {
    val tgt: Pipe[F, NJConsumerRecord[K, V], Unit] = sinks.jackson(path, hadoop, encoder, compression.fs2Compression)

    listener.fold(stream.through(tgt))(k => stream.evalTap(k.run).through(tgt))
  }
}

final class CirceDownloader[F[_], K, V] private[kafka] (
  stream: Stream[F, NJConsumerRecord[K, V]],
  hadoop: Configuration,
  path: NJPath,
  isKeepNull: Boolean,
  compression: Compression,
  listener: Option[Kleisli[F, NJConsumerRecord[K, V], Unit]]) {

  private def updateCompression(compression: Compression): CirceDownloader[F, K, V] =
    new CirceDownloader[F, K, V](stream, hadoop, path, isKeepNull, compression, listener)

  def deflate(level: Int): CirceDownloader[F, K, V] = updateCompression(Compression.Deflate(level))
  def gzip: CirceDownloader[F, K, V]                = updateCompression(Compression.Gzip)
  def uncompress: CirceDownloader[F, K, V]          = updateCompression(Compression.Uncompressed)

  def keepNull: CirceDownloader[F, K, V] =
    new CirceDownloader[F, K, V](stream, hadoop, path, true, compression, listener)
  def dropNull: CirceDownloader[F, K, V] =
    new CirceDownloader[F, K, V](stream, hadoop, path, false, compression, listener)

  def sink(implicit F: Sync[F], enc: JsonEncoder[NJConsumerRecord[K, V]]): Stream[F, Unit] = {
    val tgt: Pipe[F, NJConsumerRecord[K, V], Unit] = sinks.circe(path, hadoop, isKeepNull, compression.fs2Compression)
    listener.fold(stream.through(tgt))(k => stream.evalTap(k.run).through(tgt))
  }
}

final class ParquetDownloader[F[_], K, V] private[kafka] (
  stream: Stream[F, NJConsumerRecord[K, V]],
  encoder: AvroEncoder[NJConsumerRecord[K, V]],
  builder: AvroParquetWriter.Builder[GenericRecord],
  listener: Option[Kleisli[F, NJConsumerRecord[K, V], Unit]]) {

  def updateBuilder(f: AvroParquetWriter.Builder[GenericRecord] => AvroParquetWriter.Builder[GenericRecord])
    : ParquetDownloader[F, K, V] =
    new ParquetDownloader[F, K, V](stream, encoder, f(builder), listener)

  def sink(implicit F: Sync[F]): Stream[F, Unit] = {
    val tgt: Pipe[F, NJConsumerRecord[K, V], Unit] = sinks.parquet(builder, encoder)
    listener.fold(stream.through(tgt))(k => stream.evalTap(k.run).through(tgt))
  }
}
