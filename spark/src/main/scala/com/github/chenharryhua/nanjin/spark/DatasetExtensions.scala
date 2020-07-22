package com.github.chenharryhua.nanjin.spark

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{ConcurrentEffect, Sync}
import cats.implicits._
import cats.kernel.Eq
import com.sksamuel.avro4s.{Decoder => AvroDecoder}
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import fs2.interop.reactivestreams._
import fs2.{Pipe, Stream}
import io.circe.parser.decode
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.CsvConfiguration
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

private[spark] trait DatasetExtensions {

  implicit class RddExt[A](private val rdd: RDD[A]) {

    def dismissNulls: RDD[A] = rdd.filter(_ != null)
    def numOfNulls: Long     = rdd.subtract(dismissNulls).count()

    def stream[F[_]: Sync]: Stream[F, A] = Stream.fromIterator(rdd.toLocalIterator)

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher())

    def typedDataset(implicit ev: TypedEncoder[A], ss: SparkSession): TypedDataset[A] =
      TypedDataset.create(rdd)

    def partitionSink[F[_]: Sync, K: ClassTag: Eq](bucketing: A => K)(
      out: K => Pipe[F, A, Unit]): F[Long] = {
      val persisted: RDD[A] = rdd.persist()
      val keys: List[K]     = persisted.map(bucketing).distinct().collect().toList
      keys.traverse(k =>
        persisted.filter(a => k === bucketing(a)).stream[F].through(out(k)).compile.drain) >>
        Sync[F].delay(persisted.count()) <*
        Sync[F].delay(persisted.unpersist())
    }
  }

  implicit class TypedDatasetExt[A](private val tds: TypedDataset[A]) {

    def stream[F[_]: Sync]: Stream[F, A] = tds.dataset.rdd.stream[F]

    def source[F[_]: ConcurrentEffect]: Source[A, NotUsed] =
      Source.fromPublisher[A](stream[F].toUnicastPublisher())

    def dismissNulls: TypedDataset[A]   = tds.deserialized.filter(_ != null)
    def numOfNulls[F[_]: Sync]: F[Long] = tds.except(dismissNulls).count[F]()
  }

  implicit class DataframeExt(private val df: DataFrame) {

    def genCaseClass: String = NJDataTypeF.genCaseClass(df.schema)

  }

  implicit class SparkSessionExt(private val ss: SparkSession) {

    def withGroupId(groupId: String): SparkSession = {
      ss.sparkContext.setLocalProperty("spark.jobGroup.id", groupId)
      ss
    }

    def withDescription(description: String): SparkSession = {
      ss.sparkContext.setLocalProperty("spark.job.description", description)
      ss
    }

    def parquet[A: TypedEncoder](pathStr: String): TypedDataset[A] =
      TypedDataset.createUnsafe[A](ss.read.parquet(pathStr))

    def avro[A: ClassTag](pathStr: String)(implicit decoder: AvroDecoder[A]): RDD[A] = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setDataModelClass(job, classOf[GenericData])
      AvroJob.setInputKeySchema(job, decoder.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

      ss.sparkContext
        .newAPIHadoopFile(
          pathStr,
          classOf[AvroKeyInputFormat[GenericRecord]],
          classOf[AvroKey[GenericRecord]],
          classOf[NullWritable])
        .map { case (gr, _) => decoder.decode(gr.datum()) }
    }

    def jackson[A: ClassTag: AvroDecoder](pathStr: String): RDD[A] = {
      val schema = AvroDecoder[A].schema
      ss.sparkContext.textFile(pathStr).mapPartitions { strs =>
        val datumReader = new GenericDatumReader[GenericRecord](AvroDecoder[A].schema)
        strs.map { str =>
          val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, str)
          AvroDecoder[A].decode(datumReader.read(null, jsonDecoder))
        }
      }
    }

    def circe[A: ClassTag: JsonDecoder](pathStr: String): RDD[A] =
      ss.sparkContext
        .textFile(pathStr)
        .map(decode[A](_) match {
          case Left(ex) => throw ex
          case Right(r) => r
        })

    def csv[A: ClassTag: TypedEncoder](
      pathStr: String,
      csvConfig: CsvConfiguration): TypedDataset[A] = {
      val schema = TypedExpressionEncoder.targetStructType(TypedEncoder[A])
      TypedDataset.createUnsafe(
        ss.read
          .schema(schema)
          .option("sep", csvConfig.cellSeparator.toString)
          .option("header", csvConfig.hasHeader)
          .option("quote", csvConfig.quote.toString)
          .option("charset", "UTF8")
          .csv(pathStr))
    }

    def csv[A: ClassTag: TypedEncoder](pathStr: String): TypedDataset[A] =
      csv[A](pathStr, CsvConfiguration.rfc)

    def text(path: String): TypedDataset[String] =
      TypedDataset.create(ss.read.textFile(path))
  }
}
