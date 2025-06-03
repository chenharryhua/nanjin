package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.kernel.Eq
import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, SerdeOf}
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.sksamuel.avro4s.{Record, RecordFormat}
import fs2.kafka.{ConsumerRecord, ProducerRecord}
import org.apache.avro.generic.IndexedRecord
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord

final class TopicDef[K, V] private (val topicName: TopicName, val rawSerdes: RawKeyValueSerdePair[K, V])
    extends Serializable {

  override def toString: String = topicName.value

  def withTopicName(tn: TopicName): TopicDef[K, V] = new TopicDef[K, V](tn, rawSerdes)
  def withTopicName(tn: TopicNameL): TopicDef[K, V] = withTopicName(TopicName(tn))

  def producerRecord(k: K, v: V): ProducerRecord[K, V] = ProducerRecord(topicName.value, k, v)

  lazy val schemaPair: AvroSchemaPair =
    AvroSchemaPair(rawSerdes.key.avroCodec.schema, rawSerdes.value.avroCodec.schema)

  final class ConsumerFormat(rf: RecordFormat[NJConsumerRecord[K, V]]) extends Serializable {
    def toRecord(nj: NJConsumerRecord[K, V]): Record = rf.to(nj)
    def toRecord(cr: ConsumerRecord[K, V]): Record = toRecord(NJConsumerRecord(cr))
    def toRecord(jcr: JavaConsumerRecord[K, V]): Record = toRecord(NJConsumerRecord(jcr))

    def fromRecord(gr: IndexedRecord): NJConsumerRecord[K, V] = rf.from(gr)
  }

  final class ProducerFormat(rf: RecordFormat[NJProducerRecord[K, V]]) extends Serializable {
    def toRecord(nj: NJProducerRecord[K, V]): Record = rf.to(nj)
    def toRecord(k: K, v: V): Record = toRecord(NJProducerRecord(topicName, k, v))
    def toRecord(pr: ProducerRecord[K, V]): Record = toRecord(NJProducerRecord(pr))
    def toRecord(jpr: JavaProducerRecord[K, V]): Record = toRecord(NJProducerRecord(jpr))

    def fromRecord(gr: IndexedRecord): NJProducerRecord[K, V] = rf.from(gr)
  }

  lazy val consumerCodec: AvroCodec[NJConsumerRecord[K, V]] =
    NJConsumerRecord.avroCodec(rawSerdes.key.avroCodec, rawSerdes.value.avroCodec)

  lazy val producerCodec: AvroCodec[NJProducerRecord[K, V]] =
    NJProducerRecord.avroCodec(rawSerdes.key.avroCodec, rawSerdes.value.avroCodec)

  lazy val consumerFormat: ConsumerFormat = new ConsumerFormat(RecordFormat(consumerCodec, consumerCodec))
  lazy val producerFormat: ProducerFormat = new ProducerFormat(RecordFormat(producerCodec, producerCodec))

}

object TopicDef {

  implicit def showTopicDef[K, V]: Show[TopicDef[K, V]] = Show.fromToString

  implicit def eqTopicDef[K, V]: Eq[TopicDef[K, V]] =
    (x: TopicDef[K, V], y: TopicDef[K, V]) =>
      x.topicName.value === y.topicName.value &&
        x.rawSerdes.key.avroCodec.schema == y.rawSerdes.key.avroCodec.schema &&
        x.rawSerdes.value.avroCodec.schema == y.rawSerdes.value.avroCodec.schema

  def apply[K, V](topicName: TopicName, keySchema: AvroCodec[K], valSchema: AvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf(keySchema)
    val sv = SerdeOf(valSchema)
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }

  def apply[K: SerdeOf, V: SerdeOf](topicName: TopicName): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf[V]
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }

  def apply[K: SerdeOf, V](topicName: TopicName, valSchema: AvroCodec[V]): TopicDef[K, V] = {
    val sk = SerdeOf[K]
    val sv = SerdeOf(valSchema)
    new TopicDef(topicName, RawKeyValueSerdePair(sk, sv))
  }
}
