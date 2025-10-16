package com.github.chenharryhua.nanjin.kafka.connector

import cats.effect.kernel.*
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.{Endo, MonadError}
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.{
  AvroSchemaPair,
  AvroTopic,
  OptionalAvroSchemaPair,
  SchemaRegistrySettings
}
import com.github.chenharryhua.nanjin.messages.kafka.codec.jackson2GenericRecord
import fs2.kafka.*
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

final class ProduceGenericRecord[F[_], K, V] private[kafka] (
  avroTopic: AvroTopic[K, V],
  getSchema: F[OptionalAvroSchemaPair],
  srs: SchemaRegistrySettings,
  producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]])
    extends UpdateConfig[ProducerSettings[F, Array[Byte], Array[Byte]], ProduceGenericRecord[F, K, V]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(
    f: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]]): ProduceGenericRecord[F, K, V] =
    new ProduceGenericRecord[F, K, V](avroTopic, getSchema, srs, f(producerSettings))

  private def schemaPair(implicit F: MonadError[F, Throwable]): F[AvroSchemaPair] =
    getSchema.flatMap { skm =>
      if (avroTopic.pair.optionalSchemaPair.isBackwardCompatible(skm))
        F.pure(avroTopic.pair.optionalSchemaPair.write(skm).toPair)
      else F.raiseError(new Exception("incompatible schema"))
    }

  def schema(implicit F: MonadError[F, Throwable]): F[Schema] = schemaPair.map(_.consumerSchema)

  /*
   * sink
   */
  def sink(implicit F: Async[F]): Pipe[F, GenericRecord, ProducerResult[Array[Byte], Array[Byte]]] = {
    (grStream: Stream[F, GenericRecord]) =>
      for {
        pair <- Stream.eval(schemaPair)
        push = new PushGenericRecord(srs, avroTopic.topicName, pair)
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
      pair <- schemaPair
      gr <- F.fromTry(jackson2GenericRecord(pair.consumerSchema, jackson))
      push = new PushGenericRecord(srs, avroTopic.topicName, pair)
      res <- KafkaProducer.resource(producerSettings).use(_.produceOne(push.fromGenericRecord(gr)).flatten)
    } yield res
}
