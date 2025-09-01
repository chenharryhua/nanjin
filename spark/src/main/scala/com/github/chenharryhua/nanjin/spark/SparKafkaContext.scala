package com.github.chenharryhua.nanjin.spark

import cats.Endo
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.kafka.connector.partitionOffsetRange
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, AvroCodecOf}
import com.github.chenharryhua.nanjin.messages.kafka.{CRMetaInfo, NJConsumerRecord}
import com.github.chenharryhua.nanjin.spark.kafka.{SparKafkaTopic, Statistics}
import com.github.chenharryhua.nanjin.terminals.*
import eu.timepit.refined.refineMV
import fs2.kafka.*
import fs2.{Chunk, Stream}
import io.circe.Encoder as JsonEncoder
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.urlToUrlDsl
import org.apache.kafka.common.TopicPartition
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

  private val timeout: FiniteDuration = 15.seconds

  /** download a kafka topic and save to given folder
    *
    * @param topicName
    *   the source topic name
    * @param folder
    *   the target folder
    * @param dateRange
    *   datetime range
    */

  def dumpJackson(
    topicName: TopicName,
    folder: Url,
    dateRange: DateTimeRange,
    compression: JacksonCompression)(config: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]])(implicit
    F: Async[F]): F[Long] = {
    val file = JacksonFile(compression)
    val run: F[Stream[F, Int]] = for {
      pull <- kafkaContext.schemaRegistry
        .fetchAvroSchema(topicName)
        .map(new PullGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, _))
      range <- kafkaContext.admin(topicName).use(_.offsetRangeFor(dateRange)).map(partitionOffsetRange)
    } yield kafkaContext
      .consume(topicName)
      .updateConfig(config)
      .clientS
      .evalTap { kc =>
        kc.assign(topicName.value) *> range.toList.traverse { case (partition, (from, _)) =>
          kc.seek(new TopicPartition(topicName.value, partition), from)
        }
      }
      .flatMap { kc =>
        kc.partitionsMapStream.map { ms =>
          val streams = ms.toList.map { case (tp, stream) =>
            val (from, to) = range(tp.partition())
            val destination: Url =
              folder / s"${topicName.value}-${tp.partition()}-$from-$to.${file.fileName}"
            stream
              .takeWhile(_.record.offset < to, takeFailure = true)
              .evalMapChunk(ccr => F.fromTry(pull.toGenericRecord(ccr.record)))
              .through(hadoop.sink(destination).jackson)
          }
          streams.parJoinUnbounded.onFinalize(F.delay(println("done")) >> kc.stopConsuming)
        }.parJoinUnbounded
      }

    Stream.force(run).timeoutOnPull(timeout).compile.fold(0L) { case (s, i) => s + i }
  }

  def dumpJackson(
    topicName: TopicNameL,
    folder: Url,
    dateRange: DateTimeRange = DateTimeRange(sparkZoneId(sparkSession)))(implicit F: Async[F]): F[Long] =
    dumpJackson(TopicName(topicName), folder, dateRange, JacksonCompression.Uncompressed)(
      _.withPollInterval(0.second)
        .withEnableAutoCommit(false)
        .withIsolationLevel(IsolationLevel.ReadCommitted)
        .withMaxPollRecords(3000)
    )

  def dumpCirce[K: JsonEncoder, V: JsonEncoder](
    topicDef: TopicDef[K, V],
    folder: Url,
    dateRange: DateTimeRange,
    compression: CirceCompression)(config: Endo[ConsumerSettings[F, K, V]])(implicit F: Async[F]): F[Long] = {
    val file = CirceFile(compression)
    val run: F[Stream[F, Int]] = kafkaContext
      .admin(topicDef.topicName)
      .use(_.offsetRangeFor(dateRange).map(partitionOffsetRange))
      .map { rng =>
        kafkaContext
          .consume(topicDef)
          .updateConfig(config)
          .range(rng)
          .map { case (pr, ss) =>
            val destination: Url =
              folder / s"${topicDef.topicName.value}-${pr.partition}-${pr.from}-${pr.to}.${file.fileName}"
            ss.mapChunks(_.map(cr => NJConsumerRecord(cr.record).asJson))
              .through(hadoop.sink(destination).circe)
          }
          .foldLeft(Stream.empty.covaryAll[F, Int])(_.merge(_))
      }
    Stream.force(run).timeoutOnPull(timeout).compile.fold(0L)(_ + _)
  }

  def dumpCirce[K: JsonEncoder, V: JsonEncoder](
    topicDef: TopicDef[K, V],
    folder: Url,
    dateRange: DateTimeRange = DateTimeRange(sparkZoneId(sparkSession)))(implicit F: Async[F]): F[Long] =
    dumpCirce[K, V](topicDef, folder, dateRange, CirceCompression.Uncompressed)(
      _.withPollInterval(0.second)
        .withEnableAutoCommit(false)
        .withIsolationLevel(IsolationLevel.ReadCommitted)
        .withMaxPollRecords(3000)
    )

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
    topicName: TopicName,
    folder: Url,
    chunkSize: ChunkSize,
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]])(implicit F: Async[F]): F[Long] = {

    val producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]] =
      config(
        ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
          .withProperties(kafkaContext.settings.producerSettings.properties))

    for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      partitions <- kafkaContext.admin(topicName).use(_.partitionsFor.map(_.value.size))
      hadoop = Hadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      builder = new PushGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      num <- hadoop.filesIn(folder).flatMap { fs =>
        val step: Int = Math.ceil(fs.size.toDouble / partitions.toDouble).toInt
        if (step > 0) {
          val uploads: List[F[Long]] = fs
            .sliding(step, step)
            .map { urls =>
              val ss: Stream[F, Chunk[ProducerRecord[Array[Byte], Array[Byte]]]] = urls
                .map(hadoop.source(_).jackson(chunkSize, schemaPair.consumerSchema))
                .reduce(_ ++ _)
                .chunks
                .map(_.map(builder.fromGenericRecord))
              KafkaProducer.pipe(producerSettings).apply(ss).compile.fold(0L) { case (sum, prs) =>
                sum + prs.size
              }
            }
            .toList
          F.parSequenceN(uploads.size)(uploads).map(_.sum)
        } else F.pure(0L)
      }
    } yield num
  }

  def upload(
    topicName: TopicNameL,
    folder: Url,
    chunkSize: ChunkSize = refineMV(1000),
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]] = identity)(implicit F: Async[F]): F[Long] =
    upload(TopicName(topicName), folder, chunkSize, config)

  def crazyUpload(topicName: TopicNameL, folder: Url)(implicit F: Async[F]): F[Long] =
    upload(
      TopicName(topicName),
      folder,
      refineMV(1000),
      _.withBatchSize(200000).withLinger(10.milli).withAcks(Acks.One))

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
    topicName: TopicName,
    folder: Url,
    chunkSize: ChunkSize,
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]])(implicit F: Async[F]): F[Long] = {

    val producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]] =
      config(
        ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
          .withProperties(kafkaContext.settings.producerSettings.properties))

    for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      hadoop = Hadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      builder = new PushGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      num <- hadoop
        .filesIn(folder)
        .flatMap(
          _.traverse(
            hadoop
              .source(_)
              .jackson(chunkSize, schemaPair.consumerSchema)
              .chunks
              .map(_.map(builder.fromGenericRecord))
              .through(KafkaProducer.pipe(producerSettings))
              .compile
              .fold(0L) { case (sum, prs) => sum + prs.size }))
        .map(_.sum)
    } yield num
  }

  def sequentialUpload(
    topicName: TopicNameL,
    folder: Url,
    chunkSize: ChunkSize = refineMV(1000),
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]] = identity)(implicit F: Async[F]): F[Long] =
    sequentialUpload(TopicName(topicName), folder, chunkSize, config)

  object stats {
    private val sparkSchema: StructType = structType(AvroCodec[CRMetaInfo])

    import sparkSession.implicits.*

    def avro(folder: Url)(implicit F: Sync[F]): F[Statistics] =
      F.blocking {
        new Statistics(
          sparkSession.read
            .format("avro")
            .schema(sparkSchema)
            .load(toHadoopPath(folder).toString)
            .as[CRMetaInfo]
        )
      }

    def jackson(folder: Url)(implicit F: Sync[F]): F[Statistics] =
      F.blocking {
        new Statistics(
          sparkSession.read.schema(sparkSchema).json(toHadoopPath(folder).toString).as[CRMetaInfo]
        )
      }

    def circe(folder: Url)(implicit F: Sync[F]): F[Statistics] =
      jackson(folder)

    def parquet(folder: Url)(implicit F: Sync[F]): F[Statistics] =
      F.blocking {
        new Statistics(
          sparkSession.read.schema(sparkSchema).parquet(toHadoopPath(folder).toString).as[CRMetaInfo]
        )
      }
  }
}
