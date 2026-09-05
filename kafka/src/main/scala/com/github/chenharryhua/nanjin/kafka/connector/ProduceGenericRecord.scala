package com.github.chenharryhua.nanjin.kafka.connector

import cats.{Endo, Parallel}
import cats.effect.kernel.Async
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.admins.SchemaRegistryApi
import com.github.chenharryhua.nanjin.kafka.config.SerdeSettings
import com.github.chenharryhua.nanjin.kafka.utils.jackson2GenericRecord
import com.github.chenharryhua.nanjin.kafka.{
  AvroSchemaPair,
  OptionalAvroSchemaPair,
  SchemaIncompatible,
  TopicName
}
import fs2.kafka.*
import fs2.{Chunk, Pipe, Stream}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.RecordMetadata

/** Produces Avro `GenericRecord`s to a topic, encoding them to raw Kafka bytes.
  *
  * The producer counterpart of `ConsumeGenericRecord`: it takes Avro `GenericRecord`s, encodes the key and
  * value to `Array[Byte]` (Confluent wire format, via `PushGenericRecord`), and produces them. Before the
  * first produce it resolves and validates the write schema against the schema registry, raising
  * `SchemaIncompatible` if the caller's `schemaPair` is not backward-compatible with the registered schema.
  * Obtain an instance via `KafkaContext.produceGenericRecord(...)`.
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

  /** Return a copy with the underlying byte producer settings transformed by `f`. */
  override def updateConfig(f: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]]): ProduceGenericRecord[F] =
    new ProduceGenericRecord[F](topicName, schemaPair, srClient, serdeSettings, f(producerSettings))

  /** Resolve the effective write schema: fetch the registered schema, check the caller's `schemaPair` is
    * backward-compatible with it, and return the merged pair (preferring the broker's schema). Raises
    * `SchemaIncompatible` if the check fails.
    */
  private lazy val validateSchema: F[AvroSchemaPair] =
    SchemaRegistryApi[F](srClient)
      .fetchOptionalAvroSchema(topicName)
      .flatMap { skm =>
        if (schemaPair.isBackwardCompatible(skm))
          F.pure(schemaPair.write(skm).toSchemaPair)
        else F.raiseError(SchemaIncompatible(topicName))
      }

  /** The effective Avro schema records will be encoded against, validated against the registry. */
  lazy val schema: F[Schema] = validateSchema.map(_.consumerSchema)

  /** Chunk-oriented producer pipe: for each chunk of records, encode and produce the whole chunk, running the
    * produce effects in parallel. Validates the schema once before producing.
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

  /** Record-oriented producer pipe; chunks the input and delegates to `chunkSink`. */
  lazy val sink: Pipe[F, GenericRecord, ProducerResult[Array[Byte], Array[Byte]]] =
    _.chunks.through(chunkSink)

  /** Encode and produce a single record, returning its `RecordMetadata`. Validates the schema and opens a
    * short-lived producer for the one send.
    */
  def produceOne(record: GenericRecord): F[RecordMetadata] =
    for {
      pair <- validateSchema
      push = new PushGenericRecord(srClient, serdeSettings, topicName, pair)
      res <- KafkaProducer
        .resource(producerSettings)
        .use(_.produceOne_(push.fromGenericRecord(record)).flatten)
    } yield res

  /** Parse a Jackson-encoded Avro JSON string into a `GenericRecord` (against the resolved schema) and
    * produce it. Convenience for replaying records serialized as JSON.
    *
    * @param jackson
    *   a JSON string in Jackson/Avro form, e.g. generated from an `NJConsumerRecord`.
    */
  def jackson(jackson: String): F[RecordMetadata] =
    for {
      pair <- validateSchema
      gr <- F.fromTry(jackson2GenericRecord(pair.consumerSchema, jackson))
      res <- produceOne(gr)
    } yield res
}
