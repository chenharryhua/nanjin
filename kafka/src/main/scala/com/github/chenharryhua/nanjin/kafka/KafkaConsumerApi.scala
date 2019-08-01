package com.github.chenharryhua.nanjin.kafka

import java.time.{Duration, LocalDateTime, ZoneId}
import java.{lang, util}

import cats.Monad
import cats.data.Kleisli
import cats.effect.concurrent.MVar
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

final case class KafkaOffsetRange(fromOffset: Long, untilOffset: Long) {
  val size: Long = untilOffset - fromOffset
}

final case class ListOfTopicPartitions(value: List[TopicPartition]) extends AnyVal {

  def localDateTime2KafkaTimestamp(dt: LocalDateTime): java.lang.Long =
    dt.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli

  def timed(ldt: LocalDateTime): util.Map[TopicPartition, lang.Long] =
    value.map(tp => tp -> localDateTime2KafkaTimestamp(ldt)).toMap.asJava
  def asJava: util.List[TopicPartition] = value.asJava

}

final case class GenericTopicPartition[V](value: Map[TopicPartition, V]) extends AnyVal {
  def get(tp: TopicPartition): Option[V] = value.get(tp)

  def get(topic: String, partition: Int): Option[V] =
    value.get(new TopicPartition(topic, partition))

  def mapValues[W](f: V => W): GenericTopicPartition[W] = copy(value = value.mapValues(f))

  def combine[W](other: GenericTopicPartition[V])(fn: (V, V) => W): GenericTopicPartition[W] = {
    val ret: List[Option[(TopicPartition, W)]] =
      (value.keySet ++ other.value.keySet).toList.map { tp =>
        (value.get(tp), other.value.get(tp)).mapN((f, s) => tp -> fn(f, s))
      }
    GenericTopicPartition(ret.flatten.toMap)
  }

  def flatten[W](implicit ev: V =:= Option[W]): GenericTopicPartition[W] =
    copy(value = value.mapValues(ev).mapFilter(identity))

  def topicPartitions: ListOfTopicPartitions = ListOfTopicPartitions(value.keys.toList)
}

sealed trait KafkaPrimitiveConsumerApi[F[_]] {
  def partitionsFor: F[ListOfTopicPartitions]
  def beginningOffsets: F[GenericTopicPartition[Option[Long]]]
  def endOffsets: F[GenericTopicPartition[Option[Long]]]
  def offsetsForTimes(time: LocalDateTime): F[GenericTopicPartition[Option[OffsetAndTimestamp]]]

  def retrieveRecord(
    partition: Int,
    offset: Long): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]]
}

object KafkaPrimitiveConsumerApi {

  def apply[F[_]: Monad](
    topicName: String
  )(implicit F: ApplicativeAsk[F, KafkaConsumer[Array[Byte], Array[Byte]]])
    : KafkaPrimitiveConsumerApi[F] =
    new KafkaPrimitiveConsumerApiImpl[F](topicName)

  final private[this] class KafkaPrimitiveConsumerApiImpl[F[_]: Monad](topicName: String)(
    implicit K: ApplicativeAsk[F, KafkaConsumer[Array[Byte], Array[Byte]]]
  ) extends KafkaPrimitiveConsumerApi[F] {

    val partitionsFor: F[ListOfTopicPartitions] = {
      for {
        ret <- K.ask.map {
          _.partitionsFor(topicName).asScala.toList
            .mapFilter(Option(_))
            .map(info => new TopicPartition(topicName, info.partition))
        }
      } yield ListOfTopicPartitions(ret)
    }

    val beginningOffsets: F[GenericTopicPartition[Option[Long]]] =
      for {
        tps <- partitionsFor
        ret <- K.ask.map {
          _.beginningOffsets(tps.asJava).asScala.toMap.mapValues(v => Option(v).map(_.toLong))
        }
      } yield GenericTopicPartition(ret)

    val endOffsets: F[GenericTopicPartition[Option[Long]]] =
      for {
        tps <- partitionsFor
        ret <- K.ask.map {
          _.endOffsets(tps.asJava).asScala.toMap.mapValues(v => Option(v).map(_.toLong))
        }
      } yield GenericTopicPartition(ret)

    def offsetsForTimes(ldt: LocalDateTime): F[GenericTopicPartition[Option[OffsetAndTimestamp]]] =
      for {
        tps <- partitionsFor
        ret <- K.ask.map { _.offsetsForTimes(tps.timed(ldt)).asScala.toMap.mapValues(Option(_)) }
      } yield GenericTopicPartition(ret)

    def retrieveRecord(
      partition: Int,
      offset: Long): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      K.ask.map { consumer =>
        val tp = new TopicPartition(topicName, partition)
        consumer.assign(List(tp).asJava)
        consumer.seek(tp, offset)
        consumer.poll(Duration.ofSeconds(15)).records(tp).asScala.toList.headOption
      }
  }
}

sealed trait KafkaConsumerApi[F[_], K, V] extends KafkaPrimitiveConsumerApi[F] {

  def offsetRangeFor(
    start: LocalDateTime,
    end: LocalDateTime): F[GenericTopicPartition[KafkaOffsetRange]]
  def retrieveLastRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]
  def retrieveLastMessages: F[List[ConsumerRecord[K, V]]]

  def retrieveFirstRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]
  def retrieveFirstMessages: F[List[ConsumerRecord[K, V]]]

  def retrieveRecordsForTimes(ldt: LocalDateTime): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]
  def numOfRecords: F[GenericTopicPartition[Option[Long]]]
  def numOfRecordsSince(ldt: LocalDateTime): F[GenericTopicPartition[Option[Long]]]
}

object KafkaConsumerApi {
  type ByteArrayConsumer = KafkaConsumer[Array[Byte], Array[Byte]]

  def apply[F[_]: Concurrent, K, V](
    topicName: KafkaTopicName[K, V],
    sharedConsumer: MVar[F, ByteArrayConsumer],
    decoder: KafkaMessageDecoder[ConsumerRecord, K, V]): KafkaConsumerApi[F, K, V] =
    new KafkaConsumerApiImpl[F, K, V](topicName, sharedConsumer, decoder)

  final private[this] class KafkaConsumerApiImpl[F[_]: Concurrent, K, V](
    topicName: KafkaTopicName[K, V],
    sharedConsumer: MVar[F, ByteArrayConsumer],
    decoder: KafkaMessageDecoder[ConsumerRecord, K, V]
  ) extends KafkaConsumerApi[F, K, V] {
    import cats.mtl.implicits._
    private[this] val primitiveConsumer
      : KafkaPrimitiveConsumerApi[Kleisli[F, ByteArrayConsumer, ?]] =
      KafkaPrimitiveConsumerApi[Kleisli[F, ByteArrayConsumer, ?]](topicName.value)

    private[this] def atomically[A](r: Kleisli[F, ByteArrayConsumer, A]): F[A] =
      Sync[F].bracket(sharedConsumer.take)(r.run)(sharedConsumer.put)

    override def offsetRangeFor(
      start: LocalDateTime,
      end: LocalDateTime): F[GenericTopicPartition[KafkaOffsetRange]] =
      atomically {
        for {
          from <- primitiveConsumer.offsetsForTimes(start).map(_.mapValues(_.map(_.offset())))
          to <- primitiveConsumer.offsetsForTimes(end).map(_.mapValues(_.map(_.offset())))
          endOffset <- primitiveConsumer.endOffsets
        } yield from
          .combine(to.combine(endOffset)(_.orElse(_)))(
            (_, _).mapN((f, s) => KafkaOffsetRange(f, s)))
          .flatten
      }

    override val retrieveLastRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically {
        for {
          end <- primitiveConsumer.endOffsets
          rec <- end.value.toList.traverse {
            case (tp, of) =>
              of.filter(_ > 0)
                .flatTraverse(x => primitiveConsumer.retrieveRecord(tp.partition, x - 1))
          }
        } yield rec.flatten.sortBy(_.partition())
      }

    override val retrieveLastMessages: F[List[ConsumerRecord[K, V]]] =
      retrieveLastRecords.map(_.flatMap(r => decoder.decodeBoth(r).toOption))

    override val retrieveFirstRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically {
        for {
          beg <- primitiveConsumer.beginningOffsets
          rec <- beg.value.toList.traverse {
            case (tp, of) =>
              of.flatTraverse(x => primitiveConsumer.retrieveRecord(tp.partition, x))
          }
        } yield rec.flatten.sortBy(_.partition())
      }

    override val retrieveFirstMessages: F[List[ConsumerRecord[K, V]]] =
      retrieveFirstRecords.map(_.flatMap(r => decoder.decodeBoth(r).toOption))

    override def retrieveRecordsForTimes(
      ldt: LocalDateTime): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically {
        for {
          oft <- primitiveConsumer.offsetsForTimes(ldt)
          rec <- oft.value.toList.traverse {
            case (tp, of) =>
              of.flatTraverse(x => primitiveConsumer.retrieveRecord(tp.partition, x.offset()))
          }
        } yield rec.flatten
      }

    override val numOfRecords: F[GenericTopicPartition[Option[Long]]] =
      atomically {
        for {
          beg <- primitiveConsumer.beginningOffsets
          end <- primitiveConsumer.endOffsets
        } yield end.combine(beg)((_, _).mapN(_ - _))
      }

    override def numOfRecordsSince(ldt: LocalDateTime): F[GenericTopicPartition[Option[Long]]] =
      atomically {
        for {
          oft <- primitiveConsumer.offsetsForTimes(ldt).map(_.mapValues(_.map(_.offset())))
          end <- primitiveConsumer.endOffsets
        } yield end.combine(oft)((_, _).mapN(_ - _))
      }

    override val partitionsFor: F[ListOfTopicPartitions] =
      atomically(primitiveConsumer.partitionsFor)

    override val beginningOffsets: F[GenericTopicPartition[Option[Long]]] =
      atomically(primitiveConsumer.beginningOffsets)

    override val endOffsets: F[GenericTopicPartition[Option[Long]]] =
      atomically(primitiveConsumer.endOffsets)

    override def offsetsForTimes(
      time: LocalDateTime): F[GenericTopicPartition[Option[OffsetAndTimestamp]]] =
      atomically(primitiveConsumer.offsetsForTimes(time))

    override def retrieveRecord(
      partition: Int,
      offset: Long): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically(primitiveConsumer.retrieveRecord(partition, offset))
  }
}
