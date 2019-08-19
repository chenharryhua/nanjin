package com.github.chenharryhua.nanjin.sparkafka
import java.time.LocalDateTime

import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import frameless.TypedDataset
import org.apache.spark.sql.SparkSession

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object Sparkafka {

  def dataset[F[_], K, V](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    macro implDataset[F, K, V]

  def safeDataset[F[_], K, V](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
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

  def checkSameKeySamePartition[F[_], K, V](
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
        _root_.com.github.chenharryhua.nanjin.sparkafka.SparkafkaDataset
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
        _root_.com.github.chenharryhua.nanjin.sparkafka.SparkafkaDataset
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
        _root_.com.github.chenharryhua.nanjin.sparkafka.SparkafkaDataset
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
        _root_.com.github.chenharryhua.nanjin.sparkafka.SparkafkaDataset
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
        _root_.com.github.chenharryhua.nanjin.sparkafka.SparkafkaDataset
         .checkSameKeySamePartition($spark, $topic, $start, $end, $topic.keyIso.get, $topic.valueIso.get)
     """
  }
}
