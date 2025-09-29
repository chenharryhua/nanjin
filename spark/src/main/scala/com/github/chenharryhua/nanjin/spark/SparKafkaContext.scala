package com.github.chenharryhua.nanjin.spark

import cats.Endo
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.{CRMetaInfo, NJConsumerRecord}
import com.github.chenharryhua.nanjin.spark.kafka.Statistics
import com.github.chenharryhua.nanjin.terminals.*
import eu.timepit.refined.refineMV
import fs2.Stream
import fs2.kafka.*
import io.circe.syntax.EncoderOps
import io.circe.{Encoder as JsonEncoder, Json}
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.urlToUrlDsl
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.typelevel.cats.time.instances.zoneid

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final class SparKafkaContext[F[_]](val sparkSession: SparkSession, val kafkaContext: KafkaContext[F])
    extends Serializable with zoneid {

  val hadoop: Hadoop[F] = sparkSession.hadoop[F]

  final class DumpConfig(
    private[SparKafkaContext] val dateRange: DateTimeRange = DateTimeRange(sparkZoneId(sparkSession)),
    private[SparKafkaContext] val timeout: FiniteDuration = 15.seconds,
    private[SparKafkaContext] val ignoreError: Boolean = false,
    private[SparKafkaContext] val updateConsumerSettings: Endo[
      ConsumerSettings[F, Array[Byte], Array[Byte]]] = (_: ConsumerSettings[F, Array[Byte], Array[Byte]])
      .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576") // chatGPT recommendation
      .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000")
      .withProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "5242880")
      .withProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "104857600")
      .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000")
      .withMaxPrefetchBatches(10)
      .withIsolationLevel(IsolationLevel.ReadCommitted)
      .withPollInterval(0.second)
      .withEnableAutoCommit(false)) {
    private def copy(
      dateRange: DateTimeRange = this.dateRange,
      timeout: FiniteDuration = this.timeout,
      ignoreError: Boolean = this.ignoreError,
      updateConsumerSettings: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]] =
        this.updateConsumerSettings): DumpConfig =
      new DumpConfig(dateRange, timeout, ignoreError, updateConsumerSettings)

    def withDateTimeRange(dr: DateTimeRange): DumpConfig = copy(dateRange = dr)
    def withTimeout(fd: FiniteDuration): DumpConfig = copy(timeout = fd)
    def isIgnoreError(ignore: Boolean): DumpConfig = copy(ignoreError = ignore)

    def withConsumer(cs: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): DumpConfig =
      copy(updateConsumerSettings = cs.compose(this.updateConsumerSettings))
  }

  /** download a kafka topic and save to given folder
    *
    * @param avroTopic
    *   the source topic name
    * @param folder
    *   the target folder
    */
  def dump[K, V](avroTopic: AvroTopic[K, V], folder: Url, updateConfig: Endo[DumpConfig] = identity)(implicit
    F: Async[F]): F[Long] = {
    val config = updateConfig(new DumpConfig())
    val file = JacksonFile(_.Uncompressed)
    kafkaContext
      .consumeGenericRecord(avroTopic)
      .updateConfig(config.updateConsumerSettings)
      .circumscribedStream(config.dateRange)
      .flatMap { rs =>
        rs.partitionsMapStream.toList.map { case (pr, ss) =>
          val fn: Url = folder / s"${pr.toString}.${file.fileName}"
          val process: Stream[F, GenericData.Record] =
            if (config.ignoreError)
              ss.mapChunks(_.map(_.record.value.toOption)).unNone
            else
              ss.evalMapChunk(ccr => F.fromTry(ccr.record.value))

          process
            .through(hadoop.sink(fn).jackson)
            .adaptError(new Exception(
              Json.obj("dump.topic" -> avroTopic.topicName.asJson, "to" -> fn.asJson).noSpaces,
              _))
        }.parJoinUnbounded.onFinalize(rs.stopConsuming)
      }
      .timeoutOnPull(config.timeout)
      .compile
      .fold(0L)(_ + _)

  }

  def dumpCirce[K: JsonEncoder, V: JsonEncoder](
    topic: KafkaTopic[K, V],
    folder: Url,
    updateConfig: Endo[DumpConfig] = identity)(implicit F: Async[F]): F[Long] = {
    val config = updateConfig(new DumpConfig())
    val file = CirceFile(_.Uncompressed)
    val serde = kafkaContext.serde(topic)
    def decode(cr: ConsumerRecord[Array[Byte], Array[Byte]]): Json =
      if (config.ignoreError)
        serde.toNJConsumerRecord[ConsumerRecord](cr).toZonedJson(config.dateRange.zoneId)
      else
        NJConsumerRecord(serde.deserialize[ConsumerRecord](cr)).toZonedJson(config.dateRange.zoneId)

    kafkaContext
      .consumeBytes(topic.topicName.name)
      .updateConfig(config.updateConsumerSettings)
      .circumscribedStream(config.dateRange)
      .flatMap { rs =>
        rs.partitionsMapStream.toList.map { case (pr, ss) =>
          val fn: Url = folder / s"${pr.toString}.${file.fileName}"
          ss.mapChunks(_.map(cr => decode(cr.record)))
            .through(hadoop.sink(fn).circe)
            .adaptError(
              new Exception(Json.obj("dump.circe" -> topic.topicName.asJson, "to" -> fn.asJson).noSpaces, _))
        }.parJoinUnbounded.onFinalize(rs.stopConsuming)
      }
      .timeoutOnPull(config.timeout)
      .compile
      .fold(0L)(_ + _)

  }

  final class UploadConfig(
    private[SparKafkaContext] val chunkSize: ChunkSize = refineMV(1000),
    private[SparKafkaContext] val timeout: FiniteDuration = 15.seconds,
    private[SparKafkaContext] val updateProducerSettings: Endo[
      ProducerSettings[F, Array[Byte], Array[Byte]]] = _.withBatchSize(512 * 1024)
      .withLinger(30.milli)
      .withProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864")
      .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
  ) {
    private def copy(
      chunkSize: ChunkSize = this.chunkSize,
      timeout: FiniteDuration = this.timeout,
      updateProducerSettings: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]] =
        this.updateProducerSettings) =
      new UploadConfig(chunkSize, timeout, updateProducerSettings)

    def withTimeout(fd: FiniteDuration): UploadConfig = copy(timeout = fd)

    def withProducer(cs: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]]): UploadConfig =
      copy(updateProducerSettings = cs.compose(this.updateProducerSettings))
  }

  /** upload data from given folder to a kafka topic. files read in parallel
    *
    * @param avroTopic
    *   target topic name
    * @param folder
    *   the source data files folder
    * @return
    *   number of records uploaded
    *
    * [[https://www.conduktor.io/kafka/kafka-producer-batching/]]
    */

  def upload[K, V](avroTopic: AvroTopic[K, V], folder: Url, updateConfig: Endo[UploadConfig] = identity)(
    implicit F: Async[F]): F[Long] = {
    val config = updateConfig(new UploadConfig())
    val producer = kafkaContext.produceGenericRecord(avroTopic).updateConfig(config.updateProducerSettings)
    for {
      schema <- producer.schema
      partitions <- kafkaContext.admin(avroTopic.topicName.name).use(_.partitionsFor.map(_.value.size))
      hadoop = Hadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      num <- hadoop.filesIn(folder).flatMap { fs =>
        val step: Int = Math.ceil(fs.size.toDouble / partitions.toDouble).toInt
        if (step > 0) {
          fs.sliding(step, step)
            .map { urls =>
              urls
                .map(fn =>
                  hadoop
                    .source(fn)
                    .jackson(config.chunkSize, schema)
                    .adaptError(
                      new Exception(
                        Json.obj("upload.to" -> avroTopic.topicName.asJson, "from" -> fn.asJson).noSpaces,
                        _)
                    ))
                .reduce(_ ++ _)
                .prefetch
                .through(producer.sink)
            }
            .toList
            .parJoinUnbounded
            .timeoutOnPull(config.timeout)
            .compile
            .fold(0L) { case (sum, prs) => sum + prs.size }
        } else F.pure(0L)
      }
    } yield num
  }

  /** sequentially read files in the folder, sorted by modification time, and upload them into kafka
    *
    * @param avroTopic
    *   target topic name
    * @param folder
    *   the source data folder
    * @return
    *   number of records uploaded
    */
  def sequentialUpload[K, V](
    avroTopic: AvroTopic[K, V],
    folder: Url,
    updateConfig: Endo[UploadConfig] = identity)(implicit F: Async[F]): F[Long] = {
    val config = updateConfig(new UploadConfig())
    val producer = kafkaContext.produceGenericRecord(avroTopic).updateConfig(config.updateProducerSettings)
    for {
      schema <- producer.schema
      hadoop = Hadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      num <- hadoop
        .filesIn(folder)
        .flatMap(
          _.traverse(fn =>
            hadoop
              .source(fn)
              .jackson(config.chunkSize, schema)
              .through(producer.sink)
              .timeoutOnPull(config.timeout)
              .compile
              .fold(0L) { case (sum, prs) =>
                sum + prs.size
              }
              .adaptError(new Exception(
                Json.obj("sequential.upload.to" -> avroTopic.topicName.asJson, "from" -> fn.asJson).noSpaces,
                _))))
        .map(_.sum)
    } yield num
  }

  object stats {
    private val sparkSchema: StructType = structType(AvroCodec[CRMetaInfo])

    import sparkSession.implicits.*

    def avro(folder: Url)(implicit F: Sync[F]): F[Statistics[F]] =
      F.blocking {
        new Statistics[F](
          sparkSession.read
            .format("avro")
            .schema(sparkSchema)
            .load(toHadoopPath(folder).toString)
            .as[CRMetaInfo]
        )
      }

    def jackson(folder: Url)(implicit F: Sync[F]): F[Statistics[F]] =
      F.blocking {
        new Statistics[F](
          sparkSession.read.schema(sparkSchema).json(toHadoopPath(folder).toString).as[CRMetaInfo]
        )
      }

    def circe(folder: Url)(implicit F: Sync[F]): F[Statistics[F]] =
      jackson(folder)

    def parquet(folder: Url)(implicit F: Sync[F]): F[Statistics[F]] =
      F.blocking {
        new Statistics[F](
          sparkSession.read.schema(sparkSchema).parquet(toHadoopPath(folder).toString).as[CRMetaInfo]
        )
      }
  }
}
