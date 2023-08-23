package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.kernel.Eq
import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameC}
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJAvroCodec, SerdeOf}
import com.sksamuel.avro4s.{FromRecord, Record, ToRecord}
import org.apache.avro.generic.IndexedRecord

final class TopicDef[K, V] private (val topicName: TopicName, val rawSerdes: RawKeyValueSerdePair[K, V])
    extends Serializable {

  override def toString: String = topicName.value

  def withTopicName(tn: TopicName): TopicDef[K, V]  = new TopicDef[K, V](tn, rawSerdes)
  def withTopicName(tn: TopicNameC): TopicDef[K, V] = withTopicName(TopicName(tn))

  def withSchema(pair: AvroSchemaPair): TopicDef[K, V] =
    new TopicDef[K, V](topicName, rawSerdes.withSchema(pair))

  lazy val schemaPair: AvroSchemaPair =
    AvroSchemaPair(rawSerdes.key.avroCodec.schemaFor.schema, rawSerdes.value.avroCodec.schemaFor.schema)

  lazy val consumerRecordCodec: NJAvroCodec[NJConsumerRecord[K, V]] =
    NJConsumerRecord.avroCodec(rawSerdes.key.avroCodec, rawSerdes.value.avroCodec)

  private lazy val toGR: ToRecord[NJConsumerRecord[K, V]]     = ToRecord(consumerRecordCodec.avroEncoder)
  private lazy val fromGR: FromRecord[NJConsumerRecord[K, V]] = FromRecord(consumerRecordCodec.avroDecoder)
  def toRecord(nj: NJConsumerRecord[K, V]): Record = toGR.to(nj)
  def fromRecord(gr: IndexedRecord): NJConsumerRecord[K, V] = fromGR.from(gr)

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] = ctx.topic[K, V](this)

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
