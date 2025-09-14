package com.github.chenharryhua.nanjin.kafka.connector

import cats.effect.kernel.*
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.{Endo, Functor}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.{OptionalAvroSchemaPair, SchemaRegistrySettings}
import com.github.chenharryhua.nanjin.messages.kafka.codec.jackson2GR
import fs2.kafka.*
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

final class ProduceGenericRecord[F[_]] private[kafka] (
  topicName: TopicName,
  getSchema: F[OptionalAvroSchemaPair],
  updateSchema: Endo[OptionalAvroSchemaPair],
  srs: SchemaRegistrySettings,
  producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]])
    extends UpdateConfig[ProducerSettings[F, Array[Byte], Array[Byte]], ProduceGenericRecord[F]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]]): ProduceGenericRecord[F] =
    new ProduceGenericRecord[F](topicName, getSchema, updateSchema, srs, f(producerSettings))

  def withSchema(f: Endo[OptionalAvroSchemaPair]): ProduceGenericRecord[F] =
    new ProduceGenericRecord[F](topicName, getSchema, f, srs, producerSettings)

  def schema(implicit F: Functor[F]): F[Schema] =
    getSchema.map(updateSchema(_).toPair.consumerSchema)

  /*
   * sink
   */
  def sink(implicit F: Async[F]): Pipe[F, GenericRecord, ProducerResult[Array[Byte], Array[Byte]]] = {
    (grStream: Stream[F, GenericRecord]) =>
      for {
        pair <- Stream.eval(getSchema.map(updateSchema(_).toPair))
        push = new PushGenericRecord(srs, topicName, pair)
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
      pair <- getSchema.map(updateSchema(_).toPair)
      gr <- F.fromTry(jackson2GR(pair.consumerSchema, jackson))
      push = new PushGenericRecord(srs, topicName, pair)
      res <- KafkaProducer.resource(producerSettings).use(_.produceOne(push.fromGenericRecord(gr)).flatten)
    } yield res
}
