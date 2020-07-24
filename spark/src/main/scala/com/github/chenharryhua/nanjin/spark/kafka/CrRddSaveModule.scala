package com.github.chenharryhua.nanjin.spark.kafka

import cats.Show
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.{RddPersistMultiFile, RddPersistSingleFile}
import frameless.cats.implicits.rddOps
import io.circe.generic.auto._
import io.circe.{Encoder => JsonEncoder}

private[kafka] trait CrRddSaveModule[F[_], K, V] { self: CrRdd[F, K, V] =>

  object save {

    final class SingleFile(delegate: RddPersistSingleFile[F, OptionalKV[K, V]]) {

      def dump(implicit F: Sync[F]): F[Long] =
        delegate.dump(params.replayPath(topicName))

      def circe(implicit F: Sync[F], ek: JsonEncoder[K], ev: JsonEncoder[V]): F[Long] =
        delegate.circe(params.pathBuilder(topicName, NJFileFormat.CirceJson))

      def text(implicit F: Sync[F], showK: Show[K], showV: Show[V]): F[Long] =
        delegate.text(params.pathBuilder(topicName, NJFileFormat.Text))

      def jackson(implicit ce: ConcurrentEffect[F]): F[Long] =
        delegate.jackson(params.pathBuilder(topicName, NJFileFormat.Jackson))

      def avro(implicit ce: ConcurrentEffect[F]): F[Long] =
        delegate.avro(params.pathBuilder(topicName, NJFileFormat.Avro))

      def binAvro(implicit ce: ConcurrentEffect[F]): F[Long] =
        delegate.binAvro(params.pathBuilder(topicName, NJFileFormat.BinaryAvro))

      def parquet(implicit ce: ConcurrentEffect[F]): F[Long] =
        delegate.parquet(params.pathBuilder(topicName, NJFileFormat.Parquet))

      def javaObject(implicit ce: ConcurrentEffect[F]): F[Long] =
        delegate.javaObj(params.pathBuilder(topicName, NJFileFormat.JavaObject))
    }

    final class MultiFile(delegate: RddPersistMultiFile[F, OptionalKV[K, V]]) {

      def avro: F[Long] =
        delegate.avro(params.pathBuilder(topicName, NJFileFormat.MultiAvro))

      def jackson: F[Long] =
        delegate.jackson(params.pathBuilder(topicName, NJFileFormat.MultiJackson))
    }

    final def single(blocker: Blocker)(implicit cs: ContextShift[F]): SingleFile =
      new SingleFile(new RddPersistSingleFile[F, OptionalKV[K, V]](rdd, blocker))

    final def multi(blocker: Blocker)(implicit cs: ContextShift[F], F: Sync[F]): MultiFile =
      new MultiFile(new RddPersistMultiFile[F, OptionalKV[K, V]](rdd, blocker))
  }
}
