package com.github.chenharryhua.nanjin.kafka

import java.time.LocalDateTime

import frameless.TypedDataset
import org.apache.spark.sql.SparkSession

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object Sparkafka {

  def dataset[F[_], K, V](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime): F[TypedDataset[SparkConsumerRecord[K, V]]] =
    macro implDataset[F, K, V]

  def safeDataset[F[_], K, V](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime): F[TypedDataset[SparkConsumerRecord[K, V]]] =
    macro implSafeDataset[F, K, V]

  def valueDataset[F[_], K, V](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime): F[TypedDataset[V]] =
    macro implValueDS[F, K, V]

  def safeValueDataset[F[_], K, V](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime): F[TypedDataset[V]] =
    macro implSafeValueDS[F, K, V]

  def checkSameKeyInSamePartition[F[_], K, V](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime): F[Long] =
    macro implCheckSameKeySamePartition[F, K, V]

  def implDataset[F[_], K, V](c: blackbox.Context)(
    spark: c.Expr[SparkSession],
    topic: c.Expr[KafkaTopic[F, K, V]],
    start: c.Expr[LocalDateTime],
    end: c.Expr[LocalDateTime]): c.Tree = {
    import c.universe._
    q"""
        _root_.com.github.chenharryhua.nanjin.kafka.SparkafkaDataset
         .dataset($spark, $topic, $start, $end, $topic.keyIso.get, $topic.valueIso.get)
     """
  }

  def implSafeDataset[F[_], K, V](c: blackbox.Context)(
    spark: c.Expr[SparkSession],
    topic: c.Expr[KafkaTopic[F, K, V]],
    start: c.Expr[LocalDateTime],
    end: c.Expr[LocalDateTime]): c.Tree = {
    import c.universe._
    q"""
        _root_.com.github.chenharryhua.nanjin.kafka.SparkafkaDataset
         .safeDataset($spark, $topic, $start, $end, $topic.keyIso.get, $topic.valueIso.get)
     """
  }

  def implValueDS[F[_], K, V](c: blackbox.Context)(
    spark: c.Expr[SparkSession],
    topic: c.Expr[KafkaTopic[F, K, V]],
    start: c.Expr[LocalDateTime],
    end: c.Expr[LocalDateTime]): c.Tree = {
    import c.universe._
    q"""
        _root_.com.github.chenharryhua.nanjin.kafka.SparkafkaDataset
         .valueDataset($spark, $topic, $start, $end, $topic.valueIso.get)
     """
  }

  def implSafeValueDS[F[_], K, V](c: blackbox.Context)(
    spark: c.Expr[SparkSession],
    topic: c.Expr[KafkaTopic[F, K, V]],
    start: c.Expr[LocalDateTime],
    end: c.Expr[LocalDateTime]): c.Tree = {
    import c.universe._
    q"""
        _root_.com.github.chenharryhua.nanjin.kafka.SparkafkaDataset
         .safeValueDataset($spark, $topic, $start, $end, $topic.valueIso.get)
     """
  }

  def implCheckSameKeySamePartition[F[_], K, V](c: blackbox.Context)(
    spark: c.Expr[SparkSession],
    topic: c.Expr[KafkaTopic[F, K, V]],
    start: c.Expr[LocalDateTime],
    end: c.Expr[LocalDateTime]): c.Tree = {
    import c.universe._
    q"""
        _root_.com.github.chenharryhua.nanjin.kafka.SparkafkaDataset
         .checkSameKeyInSamePartition($spark, $topic, $start, $end, $topic.keyIso.get, $topic.valueIso.get)
     """
  }
}
