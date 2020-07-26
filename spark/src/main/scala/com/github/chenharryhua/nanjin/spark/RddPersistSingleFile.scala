package com.github.chenharryhua.nanjin.spark

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.TypedEncoder
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.RowEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class RddPersistSingleFile[F[_], A](rdd: RDD[A], blocker: Blocker)(implicit
  ss: SparkSession,
  cs: ContextShift[F]) {

  private def rddResource(implicit F: Sync[F]): Resource[F, RDD[A]] =
    Resource.make(F.delay(rdd.persist()))(r => F.delay(r.unpersist()))

  def avro(pathStr: String)(implicit enc: AvroEncoder[A], F: Sync[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink(blocker).avro[A](pathStr)).compile.drain.as(data.count)
    }

  def binAvro(pathStr: String)(implicit enc: AvroEncoder[A], F: Concurrent[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink(blocker).binAvro[A](pathStr)).compile.drain.as(data.count)
    }

  def parquet(
    pathStr: String)(implicit enc: AvroEncoder[A], ev: TypedEncoder[A], F: Sync[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink(blocker).parquet[A](pathStr)).compile.drain.as(data.count)
    }

  def jackson(pathStr: String)(implicit enc: AvroEncoder[A], F: Concurrent[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink(blocker).jackson[A](pathStr)).compile.drain.as(data.count)
    }

  def javaObj(pathStr: String)(implicit F: Concurrent[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink(blocker).javaObject[A](pathStr)).compile.drain.as(data.count)
    }

  def circe(pathStr: String)(implicit enc: JsonEncoder[A], F: Sync[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink(blocker).circe[A](pathStr)).compile.drain.as(data.count())
    }

  def csv(pathStr: String)(implicit enc: RowEncoder[A], F: Concurrent[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink(blocker).csv[A](pathStr)).compile.drain.as(data.count())
    }

  def text(pathStr: String)(implicit enc: Show[A], F: Sync[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink(blocker).text[A](pathStr)).compile.drain.as(data.count())
    }

}
