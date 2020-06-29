package com.github.chenharryhua.nanjin.spark

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.effect.{ConcurrentEffect, Sync}
import cats.implicits._
import cats.kernel.Eq
import com.sksamuel.avro4s.{Decoder => AvroDecoder}
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.interop.reactivestreams._
import fs2.{Pipe, Stream}
import io.circe.parser.decode
import io.circe.{Decoder => JsonDecoder}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
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
      keys
        .map(k => persisted.filter(a => k === bucketing(a)).stream[F].through(out(k)).compile.drain)
        .reduce(_ >> _) >>
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

    def parquet[A: TypedEncoder](pathStr: String): TypedDataset[A] =
      TypedDataset.createUnsafe[A](ss.read.parquet(pathStr))

    def avro[A: TypedEncoder](pathStr: String): TypedDataset[A] =
      TypedDataset.createUnsafe(ss.read.format("avro").load(pathStr))

    def jackson[A: AvroDecoder: ClassTag](pathStr: String): RDD[A] = {
      val schema = AvroDecoder[A].schema
      ss.sparkContext.textFile(pathStr).mapPartitions { strs =>
        val datumReader = new GenericDatumReader[GenericRecord](AvroDecoder[A].schema)
        strs.map { str =>
          val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, str)
          AvroDecoder[A].decode(datumReader.read(null, jsonDecoder))
        }
      }
    }

    def json[A: JsonDecoder: ClassTag](pathStr: String): RDD[A] =
      ss.sparkContext
        .textFile(pathStr)
        .map(decode[A](_) match {
          case Left(ex) => throw ex
          case Right(r) => r
        })

    def csv[A: TypedEncoder](pathStr: String): TypedDataset[A] =
      TypedDataset.createUnsafe[A](ss.read.csv(pathStr))

    def text(path: String): TypedDataset[String] =
      TypedDataset.create(ss.read.textFile(path))
  }
}
