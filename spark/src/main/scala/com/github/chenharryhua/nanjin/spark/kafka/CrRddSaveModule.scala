package com.github.chenharryhua.nanjin.spark.kafka

import cats.Show
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.{AvroSchema, SchemaFor, ToRecord}
import frameless.cats.implicits.rddOps
import fs2.Stream
import io.circe.generic.auto._
import io.circe.{Encoder => JsonEncoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD

private[kafka] trait CrRddSaveModule[F[_], K, V] { self: CrRdd[F, K, V] =>

  private def rddResource(implicit F: Sync[F]): Resource[F, RDD[OptionalKV[K, V]]] =
    Resource.make(F.delay(rdd.persist()))(r => F.delay(r.unpersist()))

  // dump java object
  def dump(implicit F: Sync[F], cs: ContextShift[F]): F[Long] =
    Blocker[F].use { blocker =>
      val pathStr = params.replayPath(topicName)
      fileSink(blocker).delete(pathStr) >> rddResource.use { data =>
        F.delay(data.saveAsObjectFile(pathStr)).as(data.count())
      }
    }

  //save actions
  def saveMultiAvro(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Long] = {
    val path = params.pathBuilder(topicName, NJFileFormat.MultiAvro)
    fileSink(blocker).delete(path) >> rddResource.use { data =>
      F.delay {
        implicit val ks: SchemaFor[K] = keyEncoder.schemaFor
        implicit val vs: SchemaFor[V] = valEncoder.schemaFor
        sparkSession.sparkContext.hadoopConfiguration
          .set("avro.schema.output.key", AvroSchema[OptionalKV[K, V]].toString)
        data.mapPartitions { rcds =>
          val to = ToRecord[OptionalKV[K, V]]
          rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
        }.saveAsNewAPIHadoopFile[AvroKeyOutputFormat[GenericRecord]](path)
        data.count()
      }
    }
  }

  def saveSingleCirce(blocker: Blocker)(implicit
    F: Sync[F],
    cs: ContextShift[F],
    ek: JsonEncoder[K],
    ev: JsonEncoder[V]): F[Long] = {

    val path = params.pathBuilder(topicName, NJFileFormat.CirceJson)

    rddResource.use { data =>
      data
        .stream[F]
        .through(fileSink(blocker).circe[OptionalKV[K, V]](path))
        .compile
        .drain
        .as(data.count())
    }
  }

  def saveSingleText(blocker: Blocker)(implicit
    showK: Show[K],
    showV: Show[V],
    F: Sync[F],
    cs: ContextShift[F]): F[Long] = {

    val path = params.pathBuilder(topicName, NJFileFormat.Text)

    rddResource.use { data =>
      data
        .stream[F]
        .through(fileSink(blocker).text[OptionalKV[K, V]](path))
        .compile
        .drain
        .as(data.count())
    }
  }

  private def avroLike(blocker: Blocker, fmt: NJFileFormat)(implicit
    F: ConcurrentEffect[F],
    cs: ContextShift[F]): F[Long] = {

    val path = params.pathBuilder(topicName, fmt)

    def run(data: RDD[OptionalKV[K, V]]): Stream[F, Unit] =
      fmt match {
        case NJFileFormat.Avro       => data.stream[F].through(fileSink(blocker).avro(path))
        case NJFileFormat.BinaryAvro => data.stream[F].through(fileSink(blocker).binaryAvro(path))
        case NJFileFormat.Jackson    => data.stream[F].through(fileSink(blocker).jackson(path))
        case NJFileFormat.Parquet    => data.stream[F].through(fileSink(blocker).parquet(path))
        case NJFileFormat.JavaObject => data.stream[F].through(fileSink(blocker).javaObject(path))
        case _                       => sys.error("never happen")
      }
    rddResource.use(data => run(data).compile.drain.as(data.count()))
  }

  def saveSingleJackson(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.Jackson)

  def saveSingleAvro(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.Avro)

  def saveSingleBinaryAvro(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.BinaryAvro)

  def saveSingleParquet(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.Parquet)

  def saveSingleJavaObject(
    blocker: Blocker)(implicit ce: ConcurrentEffect[F], cs: ContextShift[F]): F[Long] =
    avroLike(blocker, NJFileFormat.JavaObject)

}
