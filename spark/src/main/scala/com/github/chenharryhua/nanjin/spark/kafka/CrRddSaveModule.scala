package com.github.chenharryhua.nanjin.spark.kafka

import cats.Show
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.{RddPersistMultiFile, RddPersistSingleFile}
import frameless.TypedEncoder
import frameless.cats.implicits.rddOps
import io.circe.{Encoder => JsonEncoder}

private[kafka] trait CrRddSaveModule[F[_], K, V] { self: CrRdd[F, K, V] =>

  object save {

    final class SingleFile(delegate: RddPersistSingleFile[F, OptionalKV[K, V]]) {

      def circe(pathStr: String)(implicit F: Sync[F], ev: JsonEncoder[OptionalKV[K, V]]): F[Long] =
        delegate.circe(pathStr)

      def circe(implicit F: Sync[F], ev: JsonEncoder[OptionalKV[K, V]]): F[Long] =
        circe(params.pathBuilder(topicName, NJFileFormat.CirceJson))

      def text(pathStr: String)(implicit F: Sync[F], ev: Show[OptionalKV[K, V]]): F[Long] =
        delegate.text(pathStr)

      def text(implicit F: Sync[F], ev: Show[OptionalKV[K, V]]): F[Long] =
        text(params.pathBuilder(topicName, NJFileFormat.Text))

      def jackson(pathStr: String)(implicit ce: ConcurrentEffect[F]): F[Long] =
        delegate.jackson(pathStr)

      def jackson(implicit ce: ConcurrentEffect[F]): F[Long] =
        jackson(params.pathBuilder(topicName, NJFileFormat.Jackson))

      def avro(pathStr: String)(implicit ce: ConcurrentEffect[F]): F[Long] =
        delegate.avro(pathStr)

      def avro(implicit ce: ConcurrentEffect[F]): F[Long] =
        avro(params.pathBuilder(topicName, NJFileFormat.Avro))

      def binAvro(pathStr: String)(implicit ce: ConcurrentEffect[F]): F[Long] =
        delegate.binAvro(pathStr)

      def binAvro(implicit ce: ConcurrentEffect[F]): F[Long] =
        binAvro(params.pathBuilder(topicName, NJFileFormat.BinaryAvro))

      def parquet(pathStr: String)(implicit
        ce: ConcurrentEffect[F],
        k: TypedEncoder[K],
        v: TypedEncoder[V]): F[Long] =
        delegate.parquet(pathStr)

      def parquet(implicit
        ce: ConcurrentEffect[F],
        k: TypedEncoder[K],
        v: TypedEncoder[V]): F[Long] =
        parquet(params.pathBuilder(topicName, NJFileFormat.Parquet))

      def javaObj(pathStr: String)(implicit ce: ConcurrentEffect[F]): F[Long] =
        delegate.javaObj(pathStr)

      def javaObj(implicit ce: ConcurrentEffect[F]): F[Long] =
        javaObj(params.pathBuilder(topicName, NJFileFormat.JavaObject))
    }

    final class MultiFile(delegate: RddPersistMultiFile[F, OptionalKV[K, V]]) {

      def dump(implicit F: Sync[F]): F[Long] =
        delegate.dump(params.replayPath(topicName))

      def avro(pathStr: String): F[Long] = delegate.avro(pathStr)

      def avro: F[Long] = avro(params.pathBuilder(topicName, NJFileFormat.MultiAvro))

      def jackson(pathStr: String): F[Long] = delegate.jackson(pathStr)

      def jackson: F[Long] =
        jackson(params.pathBuilder(topicName, NJFileFormat.MultiJackson))

      def parquet(pathStr: String)(implicit k: TypedEncoder[K], v: TypedEncoder[V]): F[Long] =
        delegate.parquet(pathStr)

      def parquet(implicit k: TypedEncoder[K], v: TypedEncoder[V]): F[Long] =
        parquet(params.pathBuilder(topicName, NJFileFormat.MultiParquet))

      def circe(pathStr: String)(implicit ev: JsonEncoder[OptionalKV[K, V]]): F[Long] =
        delegate.circe(pathStr)

      def circe(implicit ev: JsonEncoder[OptionalKV[K, V]]): F[Long] =
        delegate.circe(params.pathBuilder(topicName, NJFileFormat.MultiCirce))

    }

    final def single(blocker: Blocker)(implicit cs: ContextShift[F]): SingleFile =
      new SingleFile(new RddPersistSingleFile[F, OptionalKV[K, V]](rdd, blocker))

    final def multi(blocker: Blocker)(implicit cs: ContextShift[F], F: Sync[F]): MultiFile =
      new MultiFile(new RddPersistMultiFile[F, OptionalKV[K, V]](rdd, blocker))
  }
}
