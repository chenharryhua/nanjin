package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.{RddPersistMultiFile, RddPersistSingleFile}
import frameless.TypedEncoder
import frameless.cats.implicits.rddOps

private[kafka] trait CrRddSaveModule[F[_], K, V] { self: CrRdd[F, K, V] =>

  object save {

    final def single(blocker: Blocker)(implicit cs: ContextShift[F]): SingleFile =
      new SingleFile(new RddPersistSingleFile[F, OptionalKV[K, V]](rdd, blocker))

    final def multi(blocker: Blocker)(implicit cs: ContextShift[F], F: Sync[F]): MultiFile =
      new MultiFile(new RddPersistMultiFile[F, OptionalKV[K, V]](rdd, blocker))

    final class SingleFile(saver: RddPersistSingleFile[F, OptionalKV[K, V]]) {

// 1
      def avro(pathStr: String)(implicit ce: Sync[F]): F[Long] =
        saver.avro(pathStr)

      def avro(implicit ce: Sync[F]): F[Long] =
        avro(params.pathBuilder(topicName, NJFileFormat.Avro))

// 2
      def jackson(pathStr: String)(implicit ce: ConcurrentEffect[F]): F[Long] =
        saver.jackson(pathStr)

      def jackson(implicit ce: ConcurrentEffect[F]): F[Long] =
        jackson(params.pathBuilder(topicName, NJFileFormat.Jackson))

// 3
      def parquet(pathStr: String)(implicit
        ce: ConcurrentEffect[F],
        k: TypedEncoder[K],
        v: TypedEncoder[V]): F[Long] =
        saver.parquet(pathStr)

      def parquet(implicit
        ce: ConcurrentEffect[F],
        k: TypedEncoder[K],
        v: TypedEncoder[V]): F[Long] =
        parquet(params.pathBuilder(topicName, NJFileFormat.Parquet))

    }

    final class MultiFile(saver: RddPersistMultiFile[F, OptionalKV[K, V]]) {

// 0
      def dump(implicit F: Sync[F]): F[Long] =
        saver.dump(params.replayPath(topicName))

// 1
      def avro(pathStr: String): F[Long] = saver.avro(pathStr)

      def avro: F[Long] = avro(params.pathBuilder(topicName, NJFileFormat.Avro))

// 2
      def jackson(pathStr: String): F[Long] = saver.jackson(pathStr)

      def jackson: F[Long] =
        jackson(params.pathBuilder(topicName, NJFileFormat.Jackson))

// 3
      def parquet(pathStr: String)(implicit k: TypedEncoder[K], v: TypedEncoder[V]): F[Long] =
        saver.parquet(pathStr)

      def parquet(implicit k: TypedEncoder[K], v: TypedEncoder[V]): F[Long] =
        parquet(params.pathBuilder(topicName, NJFileFormat.Parquet))

    }
  }
}
