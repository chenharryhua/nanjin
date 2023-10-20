package com.github.chenharryhua.nanjin.messages.kafka
import cats.data.Cont
import fs2.kafka.{Header, Headers, ProducerRecord}
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord
import org.apache.kafka.common.header.Header as JavaHeader

import scala.jdk.CollectionConverters.*

private[kafka] trait NJProducerRecordTransformers extends NJHeaderTransformers {
  implicit def transformJavaNJ[K, V]: Transformer[JavaProducerRecord[K, V], NJProducerRecord[K, V]] =
    (src: JavaProducerRecord[K, V]) =>
      NJProducerRecord(
        topic = src.topic(),
        partition = Option(src.partition()).map(_.toInt),
        offset = None,
        timestamp = Option(src.timestamp()).map(_.toLong),
        key = Option(src.key()),
        value = Option(src.value()),
        headers = src.headers().toArray.map(_.transformInto[NJHeader]).toList
      )

  implicit def transformNJJava[K, V]: Transformer[NJProducerRecord[K, V], JavaProducerRecord[K, V]] =
    (src: NJProducerRecord[K, V]) =>
      new JavaProducerRecord[K, V](
        src.topic,
        src.partition.map(Integer.valueOf).orNull,
        src.timestamp.map(java.lang.Long.valueOf).orNull,
        src.key.getOrElse(null.asInstanceOf[K]),
        src.value.getOrElse(null.asInstanceOf[V]),
        src.headers.map(_.transformInto[JavaHeader]).asJava
      )

  implicit def transformFs2NJ[K, V]: Transformer[ProducerRecord[K, V], NJProducerRecord[K, V]] =
    (src: ProducerRecord[K, V]) =>
      NJProducerRecord(
        topic = src.topic,
        partition = src.partition,
        offset = None,
        timestamp = src.timestamp,
        key = Option(src.key),
        value = Option(src.value),
        headers = src.headers.toChain.map(_.transformInto[NJHeader]).toList
      )

  implicit def transformNJFs2[K, V]: Transformer[NJProducerRecord[K, V], ProducerRecord[K, V]] =
    (src: NJProducerRecord[K, V]) =>
      Cont
        .pure(
          ProducerRecord[K, V](
            src.topic,
            src.key.getOrElse(null.asInstanceOf[K]),
            src.value.getOrElse(null.asInstanceOf[V])).withHeaders(
            Headers.fromSeq(src.headers.map(_.transformInto[Header]))))
        .map(pr => src.partition.fold(pr)(pr.withPartition))
        .map(pr => src.timestamp.fold(pr)(pr.withTimestamp))
        .eval
        .value
}
