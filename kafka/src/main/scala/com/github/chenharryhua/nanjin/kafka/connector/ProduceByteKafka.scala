package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.effect.kernel.*
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.SchemaRegistrySettings
import fs2.kafka.*
import fs2.{Pipe, Stream}

final class ProduceByteKafka[F[_]] private[kafka] (
  srs: SchemaRegistrySettings,
  producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]])
    extends UpdateConfig[ProducerSettings[F, Array[Byte], Array[Byte]], ProduceByteKafka[F]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]]): ProduceByteKafka[F] =
    new ProduceByteKafka[F](srs, f(producerSettings))

  /*
   * produce
   */

  def clientR(implicit F: Async[F]): Resource[F, KafkaProducer.Metrics[F, Array[Byte], Array[Byte]]] =
    KafkaProducer.resource(producerSettings)

  def clientS(implicit F: Async[F]): Stream[F, KafkaProducer.Metrics[F, Array[Byte], Array[Byte]]] =
    KafkaProducer.stream(producerSettings)

  def sink(implicit F: Async[F])
    : Pipe[F, ProducerRecords[Array[Byte], Array[Byte]], ProducerResult[Array[Byte], Array[Byte]]] =
    KafkaProducer.pipe[F, Array[Byte], Array[Byte]](producerSettings)

  def genericRecordSink(implicit F: Async[F]) = {}

}
