package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.effect.kernel.Async
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.{
  AvroSchemaPair,
  OptionalAvroSchemaPair,
  SchemaRegistrySettings,
  TopicName
}
import com.github.chenharryhua.nanjin.kafka.schema.jackson2GenericRecord
import fs2.kafka.*
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.RecordMetadata

/*
 * Produce Generic Record
 */
final class ProduceGenericRecord[F[_]] private[kafka] (
  topicName: TopicName,
  schemaPair: OptionalAvroSchemaPair,
  fromSchemaRegistry: F[OptionalAvroSchemaPair],
  srs: SchemaRegistrySettings,
  producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]])(using F: Async[F])
    extends UpdateConfig[ProducerSettings[F, Array[Byte], Array[Byte]], ProduceGenericRecord[F]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]]): ProduceGenericRecord[F] =
    new ProduceGenericRecord[F](topicName, schemaPair, fromSchemaRegistry, srs, f(producerSettings))

  private lazy val validateSchema: F[AvroSchemaPair] =
    fromSchemaRegistry.flatMap { skm =>
      if (schemaPair.isBackwardCompatible(skm))
        F.pure(schemaPair.write(skm).toSchemaPair)
      else F.raiseError(new Exception("incompatible schema"))
    }

  lazy val schema: F[Schema] = validateSchema.map(_.consumerSchema)

  /*
   * sink
   */
  lazy val sink: Pipe[F, GenericRecord, ProducerResult[Array[Byte], Array[Byte]]] = {
    (grStream: Stream[F, GenericRecord]) =>
      for {
        pair <- Stream.eval(validateSchema)
        push = new PushGenericRecord(srs, topicName, pair)
        producer <- KafkaProducer.stream(producerSettings)
        prs <- grStream.chunks
          .evalMap(grs => producer.produce(grs.map(push.fromGenericRecord)))
          .parEvalMap(Int.MaxValue)(identity)
      } yield prs
  }

  def produceOne(record: GenericRecord): F[RecordMetadata] =
    for {
      pair <- validateSchema
      push = new PushGenericRecord(srs, topicName, pair)
      res <- KafkaProducer
        .resource(producerSettings)
        .use(_.produceOne_(push.fromGenericRecord(record)).flatten)
    } yield res

  /** @param jackson
    *   a Json String generated from NJConsumerRecord
    */
  def jackson(jackson: String): F[RecordMetadata] =
    for {
      pair <- validateSchema
      gr <- F.fromTry(jackson2GenericRecord(pair.consumerSchema, jackson))
      res <- produceOne(gr)
    } yield res
}
