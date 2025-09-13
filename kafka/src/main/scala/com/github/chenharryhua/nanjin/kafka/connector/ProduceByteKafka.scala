package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.effect.kernel.*
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.messages.kafka.codec.jackson2GR
import fs2.kafka.*
import fs2.{Pipe, Stream}
import org.apache.avro.generic.GenericRecord

final class ProduceByteKafka[F[_]] private[kafka] (
  pushGenericRecord: F[PushGenericRecord],
  producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]])
    extends UpdateConfig[ProducerSettings[F, Array[Byte], Array[Byte]], ProduceByteKafka[F]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]]): ProduceByteKafka[F] =
    new ProduceByteKafka[F](pushGenericRecord, f(producerSettings))

  /*
   * sink
   */
  def sink(implicit F: Async[F]): Pipe[F, GenericRecord, ProducerResult[Array[Byte], Array[Byte]]] = {
    (grStream: Stream[F, GenericRecord]) =>
      for {
        push <- Stream.eval(pushGenericRecord)
        producer <- KafkaProducer.stream(producerSettings)
        prs <- grStream.chunks
          .evalMap(grs => producer.produce(grs.map(push.fromGenericRecord)))
          .parEvalMap(Int.MaxValue)(identity)
      } yield prs
  }

  /** @param jackson
    *   a Json String generated from NJConsumerRecord
    */
  def jackson(jackson: String)(implicit F: Async[F]): F[ProducerResult[Array[Byte], Array[Byte]]] =
    for {
      push <- pushGenericRecord
      gr <- F.fromTry(jackson2GR(push.schema, jackson))
      res <- KafkaProducer.resource(producerSettings).use(_.produceOne(push.fromGenericRecord(gr)).flatten)
    } yield res
}
