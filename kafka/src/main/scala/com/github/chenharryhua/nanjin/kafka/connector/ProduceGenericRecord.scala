package com.github.chenharryhua.nanjin.kafka.connector

import cats.Endo
import cats.effect.kernel.*
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.common.{HasProperties, UpdateConfig}
import com.github.chenharryhua.nanjin.kafka.{
  AvroSchemaPair,
  AvroTopic,
  OptionalAvroSchemaPair,
  SchemaRegistrySettings
}
import com.github.chenharryhua.nanjin.messages.kafka.codec.jackson2GenericRecord
import fs2.kafka.*
import fs2.{Chunk, Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.RecordMetadata

/*
 * Produce Generic Record
 */
final class ProduceGenericRecord[F[_], K, V] private[kafka] (
  avroTopic: AvroTopic[K, V],
  getSchema: F[OptionalAvroSchemaPair],
  srs: SchemaRegistrySettings,
  producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]])(implicit F: Async[F])
    extends UpdateConfig[ProducerSettings[F, Array[Byte], Array[Byte]], ProduceGenericRecord[F, K, V]]
    with HasProperties {

  /*
   * config
   */
  override def properties: Map[String, String] = producerSettings.properties

  override def updateConfig(
    f: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]]): ProduceGenericRecord[F, K, V] =
    new ProduceGenericRecord[F, K, V](avroTopic, getSchema, srs, f(producerSettings))

  private lazy val schemaPair: F[AvroSchemaPair] =
    getSchema.flatMap { skm =>
      if (avroTopic.pair.optionalSchemaPair.isBackwardCompatible(skm))
        F.pure(avroTopic.pair.optionalSchemaPair.write(skm).toSchemaPair)
      else F.raiseError(new Exception("incompatible schema"))
    }

  lazy val schema: F[Schema] = schemaPair.map(_.consumerSchema)

  /*
   * sink
   */
  lazy val sink: Pipe[F, GenericRecord, Chunk[RecordMetadata]] = { (grStream: Stream[F, GenericRecord]) =>
    for {
      pair <- Stream.eval(schemaPair)
      push = new PushGenericRecord(srs, avroTopic.topicName, pair)
      producer <- KafkaProducer.stream(producerSettings)
      prs <- grStream.chunks
        .evalMap(grs => producer.produce(grs.map(push.fromGenericRecord)))
        .parEvalMap(Int.MaxValue)(identity)
    } yield prs.map(_._2)
  }

  /** @param jackson
    *   a Json String generated from NJConsumerRecord
    */
  def jackson(jackson: String): F[Chunk[RecordMetadata]] =
    for {
      pair <- schemaPair
      gr <- F.fromTry(jackson2GenericRecord(pair.consumerSchema, jackson))
      push = new PushGenericRecord(srs, avroTopic.topicName, pair)
      res <- KafkaProducer.resource(producerSettings).use(_.produceOne(push.fromGenericRecord(gr)).flatten)
    } yield res.map(_._2)
}
