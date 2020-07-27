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
import scalapb.GeneratedMessage

final class RddPersistSingleFile[F[_], A](rdd: RDD[A], blocker: Blocker)(implicit
  ss: SparkSession,
  cs: ContextShift[F]) {

  private def rddResource(implicit F: Sync[F]): Resource[F, RDD[A]] =
    Resource.make(F.delay(rdd.persist()))(r => F.delay(r.unpersist()))

// 1
  def avro(pathStr: String)(implicit enc: AvroEncoder[A], F: Sync[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink[F](blocker).avro[A](pathStr)).compile.drain.as(data.count)
    }

// 2
  def jackson(pathStr: String)(implicit enc: AvroEncoder[A], F: Concurrent[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink[F](blocker).jackson[A](pathStr)).compile.drain.as(data.count)
    }

// 3
  def parquet(pathStr: String)(implicit
    enc: AvroEncoder[A],
    constraint: TypedEncoder[A],
    F: Sync[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink[F](blocker).parquet[A](pathStr)).compile.drain.as(data.count)
    }

// 4
  def circe(pathStr: String)(implicit enc: JsonEncoder[A], F: Sync[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink[F](blocker).circe[A](pathStr)).compile.drain.as(data.count())
    }

// 5
  def text(pathStr: String)(implicit enc: Show[A], F: Sync[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink[F](blocker).text[A](pathStr)).compile.drain.as(data.count())
    }

// 6
  def csv(pathStr: String)(implicit enc: RowEncoder[A], F: Concurrent[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink[F](blocker).csv[A](pathStr)).compile.drain.as(data.count())
    }

// 7
  def protobuf(pathStr: String)(implicit
    ce: Concurrent[F],
    cs: ContextShift[F],
    ev: A <:< GeneratedMessage): F[Long] =
    rddResource.use { data =>
      data
        .stream[F]
        .through(fileSink[F](blocker).protobuf[A](pathStr))
        .compile
        .drain
        .as(data.count())
    }

// 8
  def binAvro(pathStr: String)(implicit enc: AvroEncoder[A], F: Concurrent[F]): F[Long] =
    rddResource.use { data =>
      data.stream[F].through(fileSink[F](blocker).binAvro[A](pathStr)).compile.drain.as(data.count)
    }

// 9
  def javaObj(pathStr: String)(implicit F: Concurrent[F]): F[Long] =
    rddResource.use { data =>
      data
        .stream[F]
        .through(fileSink[F](blocker).javaObject[A](pathStr))
        .compile
        .drain
        .as(data.count)
    }
}
