package com.github.chenharryhua.nanjin.spark.kafka

import alleycats.Empty
import cats.implicits.toShow
import cats.{Bifunctor, Eq, Show}
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import fs2.kafka.{ProducerRecord => Fs2ProducerRecord}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import monocle.macros.Lenses
import org.apache.kafka.clients.producer.ProducerRecord
import shapeless.cachedImplicit

@Lenses final case class NJProducerRecord[K, V](
  partition: Option[Int],
  offset: Option[Long], // for sort
  timestamp: Option[Long],
  key: Option[K],
  value: Option[V]) {

  def newPartition(pt: Int): NJProducerRecord[K, V]  = NJProducerRecord.partition.set(Some(pt))(this)
  def newTimestamp(ts: Long): NJProducerRecord[K, V] = NJProducerRecord.timestamp.set(Some(ts))(this)
  def newKey(k: K): NJProducerRecord[K, V]           = NJProducerRecord.key.set(Some(k))(this)
  def newValue(v: V): NJProducerRecord[K, V]         = NJProducerRecord.value.set(Some(v))(this)

  def noPartition: NJProducerRecord[K, V] = NJProducerRecord.partition.set(None)(this)
  def noTimestamp: NJProducerRecord[K, V] = NJProducerRecord.timestamp.set(None)(this)

  def noMeta: NJProducerRecord[K, V] =
    NJProducerRecord.timestamp[K, V].set(None).andThen(NJProducerRecord.partition[K, V].set(None))(this)

  def modifyKey(f: K => K): NJProducerRecord[K, V] =
    NJProducerRecord.key.modify((_: Option[K]).map(f))(this)

  def modifyValue(f: V => V): NJProducerRecord[K, V] =
    NJProducerRecord.value.modify((_: Option[V]).map(f))(this)

  @SuppressWarnings(Array("AsInstanceOf"))
  def toFs2ProducerRecord(topicName: String): Fs2ProducerRecord[K, V] = {
    val pr = Fs2ProducerRecord(topicName, key.getOrElse(null.asInstanceOf[K]), value.getOrElse(null.asInstanceOf[V]))
    (partition, timestamp) match {
      case (None, None)       => pr
      case (Some(p), None)    => pr.withPartition(p)
      case (None, Some(t))    => pr.withTimestamp(t)
      case (Some(p), Some(t)) => pr.withPartition(p).withTimestamp(t)
    }
  }
}

object NJProducerRecord {

  def apply[K, V](pr: ProducerRecord[Option[K], Option[V]]): NJProducerRecord[K, V] =
    NJProducerRecord(Option(pr.partition), None, Option(pr.timestamp), pr.key, pr.value)

  def apply[K, V](k: K, v: V): NJProducerRecord[K, V] =
    NJProducerRecord(None, None, None, Option(k), Option(v))

  def apply[K, V](v: V): NJProducerRecord[K, V] =
    NJProducerRecord(None, None, None, None, Option(v))

  def avroCodec[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V]): AvroCodec[NJProducerRecord[K, V]] = {
    implicit val schemaForKey: SchemaFor[K]  = keyCodec.schemaFor
    implicit val schemaForVal: SchemaFor[V]  = valCodec.schemaFor
    implicit val keyDecoder: Decoder[K]      = keyCodec.avroDecoder
    implicit val valDecoder: Decoder[V]      = valCodec.avroDecoder
    implicit val keyEncoder: Encoder[K]      = keyCodec.avroEncoder
    implicit val valEncoder: Encoder[V]      = valCodec.avroEncoder
    val s: SchemaFor[NJProducerRecord[K, V]] = cachedImplicit
    val d: Decoder[NJProducerRecord[K, V]]   = cachedImplicit
    val e: Encoder[NJProducerRecord[K, V]]   = cachedImplicit
    AvroCodec[NJProducerRecord[K, V]](s, d, e)
  }

  def avroCodec[K, V](topicDef: TopicDef[K, V]): AvroCodec[NJProducerRecord[K, V]] =
    avroCodec(topicDef.serdeOfKey.avroCodec, topicDef.serdeOfVal.avroCodec)

  implicit def eqNJProducerRecord[K: Eq, V: Eq]: Eq[NJProducerRecord[K, V]] =
    cats.derived.semiauto.eq[NJProducerRecord[K, V]]

  implicit def emptyNJProducerRecord[K, V]: Empty[NJProducerRecord[K, V]] =
    new Empty[NJProducerRecord[K, V]] {
      override val empty: NJProducerRecord[K, V] = NJProducerRecord(None, None, None, None, None)
    }

  implicit def jsonEncoderNJProducerRecord[K: JsonEncoder, V: JsonEncoder]: JsonEncoder[NJProducerRecord[K, V]] =
    deriveEncoder[NJProducerRecord[K, V]]

  implicit def jsonDecoderNJProducerRecord[K: JsonDecoder, V: JsonDecoder]: JsonDecoder[NJProducerRecord[K, V]] =
    deriveDecoder[NJProducerRecord[K, V]]

  implicit val bifunctorNJProducerRecord: Bifunctor[NJProducerRecord] =
    new Bifunctor[NJProducerRecord] {

      override def bimap[A, B, C, D](fab: NJProducerRecord[A, B])(f: A => C, g: B => D): NJProducerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  implicit def showNJProducerRecord[K: Show, V: Show]: Show[NJProducerRecord[K, V]] =
    nj => s"PR(k=${nj.key.show},v=${nj.value.show})"
}
