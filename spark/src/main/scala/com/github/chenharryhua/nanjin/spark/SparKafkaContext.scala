package com.github.chenharryhua.nanjin.spark

import cats.Endo
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.CRMetaInfo
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, AvroCodecOf}
import com.github.chenharryhua.nanjin.spark.kafka.{SparKafkaTopic, Statistics}
import com.github.chenharryhua.nanjin.terminals.*
import eu.timepit.refined.refineMV
import fs2.kafka.*
import io.circe.Encoder as JsonEncoder
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.urlToUrlDsl
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.typelevel.cats.time.instances.zoneid

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final class SparKafkaContext[F[_]](val sparkSession: SparkSession, val kafkaContext: KafkaContext[F])
    extends Serializable with zoneid {

  val hadoop: Hadoop[F] = sparkSession.hadoop[F]

  def topic[K, V](topicDef: TopicDef[K, V]): SparKafkaTopic[F, K, V] =
    new SparKafkaTopic[F, K, V](sparkSession, kafkaContext, topicDef)

  def topic[K: AvroCodecOf, V: AvroCodecOf](topicName: TopicName): SparKafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  def topic[K: AvroCodecOf, V: AvroCodecOf](topicName: TopicNameL): SparKafkaTopic[F, K, V] =
    topic[K, V](TopicName(topicName))

  final class DumpConfig(
    private[spark] val dateRange: DateTimeRange = DateTimeRange(sparkZoneId(sparkSession)),
    private[spark] val updateSchema: Endo[OptionalAvroSchemaPair] = identity,
    private[spark] val timeout: FiniteDuration = 15.seconds,
    private[spark] val ignoreError: Boolean = false,
    private[spark] val updateConsumerSettings: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]] =
      (_: ConsumerSettings[F, Array[Byte], Array[Byte]])
        .withPollInterval(0.second)
        .withEnableAutoCommit(false)
        .withIsolationLevel(IsolationLevel.ReadCommitted)
        .withMaxPollRecords(3000)) {
    private def copy(
      dateRange: DateTimeRange = this.dateRange,
      updateSchema: Endo[OptionalAvroSchemaPair] = this.updateSchema,
      timeout: FiniteDuration = this.timeout,
      ignoreError: Boolean = this.ignoreError,
      updateConsumerSettings: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]] =
        this.updateConsumerSettings): DumpConfig =
      new DumpConfig(dateRange, updateSchema, timeout, ignoreError, updateConsumerSettings)

    def withDateTimeRange(dr: DateTimeRange): DumpConfig = copy(dateRange = dr)
    def withSchema(f: Endo[OptionalAvroSchemaPair]): DumpConfig = copy(updateSchema = f)
    def withTimeout(fd: FiniteDuration): DumpConfig = copy(timeout = fd)
    def isIgnoreError(ignore: Boolean): DumpConfig = copy(ignoreError = ignore)
    def withConsumer(cs: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): DumpConfig =
      copy(updateConsumerSettings = cs.compose(this.updateConsumerSettings))
  }

  /** download a kafka topic and save to given folder
    *
    * @param topicName
    *   the source topic name
    * @param folder
    *   the target folder
    */
  def dump(topicName: TopicNameL, folder: Url, updateConfig: Endo[DumpConfig] = identity)(implicit
    F: Async[F]): F[Long] = {
    val config = updateConfig(new DumpConfig())
    val file = JacksonFile(_.Uncompressed)
    kafkaContext
      .consume(topicName)
      .updateConfig(config.updateConsumerSettings)
      .withSchema(config.updateSchema)
      .circumscribedStream(config.dateRange)
      .flatMap { rs =>
        rs.partitionsMapStream.toList.map { case (pr, ss) =>
          val sink = hadoop.sink(folder / s"${pr.toString}.${file.fileName}").jackson
          if (config.ignoreError)
            ss.mapChunks(_.map(_.record.value.toOption)).unNone.through(sink)
          else
            ss.evalMapChunk(ccr => F.fromTry(ccr.record.value)).through(sink)
        }.parJoinUnbounded.onFinalize(rs.stopConsuming)
      }
      .timeoutOnPull(config.timeout)
      .compile
      .fold(0L)(_ + _)
  }

  def dumpCirce[K: JsonEncoder, V: JsonEncoder](
    topicDef: TopicDef[K, V],
    folder: Url,
    updateConfig: Endo[DumpConfig] = identity)(implicit F: Async[F]): F[Long] = {
    val config = updateConfig(new DumpConfig())
    val file = CirceFile(_.Uncompressed)
    kafkaContext
      .consume(topicDef.topicName.name)
      .updateConfig(config.updateConsumerSettings)
      .withSchema(config.updateSchema)
      .withSchema(_.withKeySchema(topicDef.schemaPair.key).withValSchema(topicDef.schemaPair.value))
      .circumscribedStream(config.dateRange)
      .flatMap { rs =>
        rs.partitionsMapStream.toList.map { case (pr, ss) =>
          val sink = hadoop.sink(folder / s"${pr.toString}.${file.fileName}").circe
          if (config.ignoreError)
            ss.mapChunks(_.map(_.record.value.toOption.map(
              topicDef.consumerFormat.fromRecord(_).zonedJson(config.dateRange.zoneId))))
              .unNone
              .through(sink)
          else
            ss.evalMapChunk(ccr =>
              F.fromTry(ccr.record.value.map(
                topicDef.consumerFormat.fromRecord(_).zonedJson(config.dateRange.zoneId))))
              .through(sink)
        }.parJoinUnbounded.onFinalize(rs.stopConsuming)
      }
      .timeoutOnPull(config.timeout)
      .compile
      .fold(0L)(_ + _)
  }

  /** upload data from given folder to a kafka topic. files read in parallel
    *
    * @param topicName
    *   target topic name
    * @param folder
    *   the source data files folder
    * @param chunkSize
    *   when set to 1, the data will be sent in order
    * @param config
    *   config fs2.kafka.producer. Acks.All for reliable upload
    * @return
    *   number of records uploaded
    *
    * [[https://www.conduktor.io/kafka/kafka-producer-batching/]]
    */

  def upload(
    topicName: TopicNameL,
    folder: Url,
    chunkSize: ChunkSize = refineMV(1000),
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]] =
      _.withBatchSize(512 * 1024).withLinger(50.milli))(implicit F: Async[F]): F[Long] =
    for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      partitions <- kafkaContext.admin(topicName).use(_.partitionsFor.map(_.value.size))
      hadoop = Hadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      sink = kafkaContext.produce(topicName).updateConfig(config).sink
      num <- hadoop.filesIn(folder).flatMap { fs =>
        val step: Int = Math.ceil(fs.size.toDouble / partitions.toDouble).toInt
        if (step > 0) {
          fs.sliding(step, step)
            .map { urls =>
              urls
                .map(hadoop.source(_).jackson(chunkSize, schemaPair.consumerSchema))
                .reduce(_ ++ _)
                .prefetch
                .through(sink)
            }
            .toList
            .parJoinUnbounded
            .compile
            .fold(0L) { case (sum, prs) => sum + prs.size }
        } else F.pure(0L)
      }
    } yield num

  def crazyUpload(topicName: TopicNameL, folder: Url)(implicit F: Async[F]): F[Long] =
    upload(
      topicName,
      folder,
      refineMV(1000),
      _.withBatchSize(512 * 1024).withLinger(50.milli).withAcks(Acks.One))

  /** sequentially read files in the folder, sorted by modification time, and upload them into kafka
    *
    * @param topicName
    *   target topic name
    * @param folder
    *   the source data folder
    * @param chunkSize
    *   when set to 1, the data will be sent in order
    * @param config
    *   config fs2.kafka.producer. Acks.All for reliable upload
    * @return
    *   number of records uploaded
    */
  def sequentialUpload(
    topicName: TopicNameL,
    folder: Url,
    chunkSize: ChunkSize = refineMV(1000),
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]] = identity)(implicit F: Async[F]): F[Long] =

    for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      hadoop = Hadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      sink = kafkaContext.produce(topicName).updateConfig(config).sink
      num <- hadoop
        .filesIn(folder)
        .flatMap(_.traverse(
          hadoop.source(_).jackson(chunkSize, schemaPair.consumerSchema).through(sink).compile.fold(0L) {
            case (sum, prs) => sum + prs.size
          }))
        .map(_.sum)
    } yield num

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
