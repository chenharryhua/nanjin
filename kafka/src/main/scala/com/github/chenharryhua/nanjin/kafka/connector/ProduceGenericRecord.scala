package com.github.chenharryhua.nanjin.kafka.connector

import cats.{Endo, Parallel}
import cats.effect.kernel.Async
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.admins.SchemaRegistryApi
import com.github.chenharryhua.nanjin.kafka.schema.jackson2GenericRecord
import com.github.chenharryhua.nanjin.kafka.{AvroSchemaPair, OptionalAvroSchemaPair, TopicName}
import fs2.kafka.*
import fs2.{Chunk, Pipe, Stream}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.RecordMetadata
import com.github.chenharryhua.nanjin.kafka.SerdeSettings

/*
 * Produce Generic Record
 */
final class ProduceGenericRecord[F[_]: Parallel] private[kafka] (
  topicName: TopicName,
  schemaPair: OptionalAvroSchemaPair,
  srClient: SchemaRegistryClient,
  serdeSettings: SerdeSettings,
  producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]])(using F: Async[F])
    extends UpdateConfig[ProducerSettings[F, Array[Byte], Array[Byte]], ProduceGenericRecord[F]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(f: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]]): ProduceGenericRecord[F] =
    new ProduceGenericRecord[F](topicName, schemaPair, srClient, serdeSettings, f(producerSettings))

  private lazy val validateSchema: F[AvroSchemaPair] =
    SchemaRegistryApi[F](srClient)
      .fetchOptionalAvroSchema(topicName)
      .flatMap { skm =>
        if (schemaPair.isBackwardCompatible(skm))
          F.pure(schemaPair.write(skm).toSchemaPair)
        else F.raiseError(new Exception("incompatible schema"))
      }

  lazy val schema: F[Schema] = validateSchema.map(_.consumerSchema)

  /*
   * sink
   */
  lazy val chunkSink: Pipe[F, Chunk[GenericRecord], ProducerResult[Array[Byte], Array[Byte]]] = {
    (grStream: Stream[F, Chunk[GenericRecord]]) =>
      for {
        pair <- Stream.eval(validateSchema)
        push = new PushGenericRecord(srClient, serdeSettings, topicName, pair)
        producer <- KafkaProducer.stream(producerSettings)
        prs <- grStream
          .evalMap(grs => producer.produce(grs.map(push.fromGenericRecord)))
          .parEvalMap(Int.MaxValue)(identity)
      } yield prs
  }

  lazy val sink: Pipe[F, GenericRecord, ProducerResult[Array[Byte], Array[Byte]]] =
    _.chunks.through(chunkSink)

  def produceOne(record: GenericRecord): F[RecordMetadata] =
    for {
      pair <- validateSchema
      push = new PushGenericRecord(srClient, serdeSettings, topicName, pair)
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
