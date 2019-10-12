package com.github.chenharryhua.nanjin.kafka

import java.time.{Duration, LocalDateTime}

import cats.data.Kleisli
import cats.effect.concurrent.MVar
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import cats.tagless._
import cats.{Eval, Monad}
import fs2.kafka.KafkaByteConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.util.Try

@autoFunctorK
@autoSemigroupalK
@autoProductNK
sealed trait KafkaPrimitiveConsumerApi[F[_]] {

  def partitionsFor: F[ListOfTopicPartitions]
  def beginningOffsets: F[GenericTopicPartition[Option[KafkaOffset]]]
  def endOffsets: F[GenericTopicPartition[Option[KafkaOffset]]]
  def offsetsForTimes(ts: KafkaTimestamp): F[GenericTopicPartition[Option[KafkaOffset]]]

  def retrieveRecord(
    partition: KafkaPartition,
    offset: KafkaOffset): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]]
}

object KafkaPrimitiveConsumerApi {

  def apply[F[_]: Monad](topicName: String)(
    implicit F: ApplicativeAsk[F, KafkaByteConsumer]): KafkaPrimitiveConsumerApi[F] =
    new KafkaPrimitiveConsumerApiImpl[F](topicName)

  final private[this] class KafkaPrimitiveConsumerApiImpl[F[_]: Monad](topicName: String)(
    implicit kbc: ApplicativeAsk[F, KafkaByteConsumer]
  ) extends KafkaPrimitiveConsumerApi[F] {

    val partitionsFor: F[ListOfTopicPartitions] = {
      for {
        ret <- kbc.ask.map { c =>
          Try(c.partitionsFor(topicName).asScala.toList).toOption.sequence.flatten
            .mapFilter(Option(_))
            .map(info => new TopicPartition(topicName, info.partition))
        }
      } yield ListOfTopicPartitions(ret)
    }

    val beginningOffsets: F[GenericTopicPartition[Option[KafkaOffset]]] =
      for {
        tps <- partitionsFor
        ret <- kbc.ask.map {
          _.beginningOffsets(tps.asJava).asScala.toMap.mapValues(v =>
            Option(v).map(x => KafkaOffset(x.toLong)))
        }
      } yield GenericTopicPartition(ret)

    val endOffsets: F[GenericTopicPartition[Option[KafkaOffset]]] =
      for {
        tps <- partitionsFor
        ret <- kbc.ask.map {
          _.endOffsets(tps.asJava).asScala.toMap.mapValues(v =>
            Option(v).map(x => KafkaOffset(x.toLong)))
        }
      } yield GenericTopicPartition(ret)

    override def offsetsForTimes(
      ts: KafkaTimestamp): F[GenericTopicPartition[Option[KafkaOffset]]] =
      for {
        tps <- partitionsFor
        ret <- kbc.ask.map {
          _.offsetsForTimes(tps.javaTimed(ts)).asScala.toMap.mapValues(Option(_).map(x =>
            KafkaOffset(x.offset())))
        }
      } yield GenericTopicPartition(ret)

    def retrieveRecord(
      partition: KafkaPartition,
      offset: KafkaOffset): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      kbc.ask.map { consumer =>
        val tp = new TopicPartition(topicName, partition.value)
        consumer.assign(List(tp).asJava)
        consumer.seek(tp, offset.value)
        consumer.poll(Duration.ofSeconds(15)).records(tp).asScala.toList.headOption
      }
  }
}

@autoFunctorK
@autoSemigroupalK
sealed trait KafkaConsumerApi[F[_], K, V] extends KafkaPrimitiveConsumerApi[F] {

  def offsetRangeFor(dtr: KafkaDateTimeRange): F[GenericTopicPartition[KafkaOffsetRange]]

  final def offsetRangeFor(
    start: LocalDateTime,
    end: LocalDateTime): F[GenericTopicPartition[KafkaOffsetRange]] =
    offsetRangeFor(KafkaDateTimeRange(Some(KafkaTimestamp(start)), Some(KafkaTimestamp(end))))

  def retrieveLastRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]
  def retrieveFirstRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]

  def retrieveRecordsForTimes(ts: KafkaTimestamp): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]

  def numOfRecords: F[GenericTopicPartition[Option[KafkaOffsetRange]]]
  def numOfRecordsSince(ts: KafkaTimestamp): F[GenericTopicPartition[Option[KafkaOffsetRange]]]

  final def numOfRecordsSince(
    ldt: LocalDateTime): F[GenericTopicPartition[Option[KafkaOffsetRange]]] =
    numOfRecordsSince(KafkaTimestamp(ldt))
}

object KafkaConsumerApi {

  def apply[F[_]: Concurrent, K, V](topic: KafkaTopic[F, K, V]): KafkaConsumerApi[F, K, V] =
    new KafkaConsumerApiImpl[F, K, V](topic)

  final private[this] class KafkaConsumerApiImpl[F[_]: Concurrent, K, V](topic: KafkaTopic[F, K, V])
      extends KafkaConsumerApi[F, K, V] {
    import cats.mtl.implicits._

    private val topicName: String                                = topic.topicDef.topicName
    private val sharedConsumer: Eval[MVar[F, KafkaByteConsumer]] = topic.sharedConsumer

    private[this] val kpc: KafkaPrimitiveConsumerApi[Kleisli[F, KafkaByteConsumer, *]] =
      KafkaPrimitiveConsumerApi[Kleisli[F, KafkaByteConsumer, *]](topicName)

    private[this] def atomically[A](r: Kleisli[F, KafkaByteConsumer, A]): F[A] =
      Sync[F].bracket(sharedConsumer.value.take)(r.run)(sharedConsumer.value.put)

    override def offsetRangeFor(
      dtr: KafkaDateTimeRange): F[GenericTopicPartition[KafkaOffsetRange]] =
      atomically {
        for {
          from <- dtr.start.fold(kpc.beginningOffsets)(kpc.offsetsForTimes)
          to <- dtr.end.fold(kpc.endOffsets)(kpc.offsetsForTimes)
        } yield from
          .combineWith(to)((_, _).mapN((f, s) => KafkaOffsetRange(f, s)))
          .flatten[KafkaOffsetRange]
      }

    override def retrieveLastRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically {
        for {
          end <- kpc.endOffsets
          rec <- end.value.toList.traverse {
            case (tp, of) =>
              of.filter(_.value > 0)
                .flatTraverse(x => kpc.retrieveRecord(KafkaPartition(tp.partition), x.asLast))
          }
        } yield rec.flatten.sortBy(_.partition())
      }

    override def retrieveFirstRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically {
        for {
          beg <- kpc.beginningOffsets
          rec <- beg.value.toList.traverse {
            case (tp, of) =>
              of.flatTraverse(x => kpc.retrieveRecord(KafkaPartition(tp.partition), x))
          }
        } yield rec.flatten.sortBy(_.partition())
      }

    override def retrieveRecordsForTimes(
      ts: KafkaTimestamp): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically {
        for {
          oft <- kpc.offsetsForTimes(ts)
          rec <- oft.value.toList.traverse {
            case (tp, of) =>
              of.flatTraverse(x => kpc.retrieveRecord(KafkaPartition(tp.partition), x))
          }
        } yield rec.flatten
      }

    override def numOfRecords: F[GenericTopicPartition[Option[KafkaOffsetRange]]] =
      atomically {
        for {
          beg <- kpc.beginningOffsets
          end <- kpc.endOffsets
        } yield beg.combineWith(end)((_, _).mapN(KafkaOffsetRange(_, _)))
      }

    override def numOfRecordsSince(
      ts: KafkaTimestamp): F[GenericTopicPartition[Option[KafkaOffsetRange]]] =
      atomically {
        for {
          oft <- kpc.offsetsForTimes(ts)
          end <- kpc.endOffsets
        } yield oft.combineWith(end)((_, _).mapN(KafkaOffsetRange(_, _)))
      }

    override def partitionsFor: F[ListOfTopicPartitions] =
      atomically(kpc.partitionsFor)

    override def beginningOffsets: F[GenericTopicPartition[Option[KafkaOffset]]] =
      atomically(kpc.beginningOffsets)

    override def endOffsets: F[GenericTopicPartition[Option[KafkaOffset]]] =
      atomically(kpc.endOffsets)

    override def offsetsForTimes(
      ts: KafkaTimestamp): F[GenericTopicPartition[Option[KafkaOffset]]] =
      atomically(kpc.offsetsForTimes(ts))

    override def retrieveRecord(
      partition: KafkaPartition,
      offset: KafkaOffset): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically(kpc.retrieveRecord(partition, offset))
  }
}
