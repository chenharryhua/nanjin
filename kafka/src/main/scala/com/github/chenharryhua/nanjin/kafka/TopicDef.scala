package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.kernel.Eq
import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJAvroCodec, SerdeOf}
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.sksamuel.avro4s.{FromRecord, Record, ToRecord}
import org.apache.avro.generic.IndexedRecord

final class TopicDef[K, V] private (val topicName: TopicName, val rawSerdes: RawKeyValueSerdePair[K, V])
    extends Serializable {

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] = ctx.topic[K, V](this)

  override def toString: String = topicName.value

  def withTopicName(tn: TopicName): TopicDef[K, V]  = new TopicDef[K, V](tn, rawSerdes)
  def withTopicName(tn: TopicNameL): TopicDef[K, V] = withTopicName(TopicName(tn))

  def withSchema(pair: AvroSchemaPair): TopicDef[K, V] =
    new TopicDef[K, V](topicName, rawSerdes.withSchema(pair))

  lazy val schemaPair: AvroSchemaPair =
    AvroSchemaPair(rawSerdes.key.avroCodec.schemaFor.schema, rawSerdes.value.avroCodec.schemaFor.schema)

  // consumer
  final class ConsumerRecordCodec(val codec: NJAvroCodec[NJConsumerRecord[K, V]]) extends Serializable {
    private val toGR: ToRecord[NJConsumerRecord[K, V]]     = ToRecord(codec.avroEncoder)
    private val fromGR: FromRecord[NJConsumerRecord[K, V]] = FromRecord(codec.avroDecoder)

    def toRecord(nj: NJConsumerRecord[K, V]): Record          = toGR.to(nj)
    def fromRecord(gr: IndexedRecord): NJConsumerRecord[K, V] = fromGR.from(gr)
  }

  lazy val consumerRecord: ConsumerRecordCodec =
    new ConsumerRecordCodec(NJConsumerRecord.avroCodec(rawSerdes.key.avroCodec, rawSerdes.value.avroCodec))

  // producer
  final class ProducerRecordCodec(val codec: NJAvroCodec[NJProducerRecord[K, V]]) extends Serializable {
    private val toGR: ToRecord[NJProducerRecord[K, V]]     = ToRecord(codec.avroEncoder)
    private val fromGR: FromRecord[NJProducerRecord[K, V]] = FromRecord(codec.avroDecoder)

    def toRecord(nj: NJProducerRecord[K, V]): Record          = toGR.to(nj)
    def toRecord(k: K, v: V): Record                          = toGR.to(NJProducerRecord(topicName, k, v))
    def fromRecord(gr: IndexedRecord): NJProducerRecord[K, V] = fromGR.from(gr)
  }

  lazy val producerRecord: ProducerRecordCodec =
    new ProducerRecordCodec(NJProducerRecord.avroCodec(rawSerdes.key.avroCodec, rawSerdes.value.avroCodec))

}

object TopicDef {

  implicit def showTopicDef[K, V]: Show[TopicDef[K, V]] = _.toString

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName.value === y.topicName.value &&
        x.rawSerdes.key.avroCodec.schema == y.rawSerdes.key.avroCodec.schema &&
        x.rawSerdes.value.avroCodec.schema == y.rawSerdes.value.avroCodec.schema

  def apply[K, V](
    topicName: TopicName,
    keySchema: NJAvroCodec[K],
    valSchema: NJAvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf(keySchema)
    val sv = SerdeOf(valSchema)
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }

  def apply[K: SerdeOf, V: SerdeOf](topicName: TopicName): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf[V]
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }

  def apply[K: SerdeOf, V](topicName: TopicName, valSchema: NJAvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf(valSchema)
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }
}
