package com.github.chenharryhua.nanjin.sparkafka
import java.time.LocalDateTime

import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object Sparkafka {

  def kafkaDS[F[_], K, V](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime): F[Dataset[(K, V)]] = macro Sparkafka.impl[F, K, V]

  def impl[F[_], K, V](c: blackbox.Context)(
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
}
