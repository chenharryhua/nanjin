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
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndTimestamp}
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
  def offsetsForTimes(time: LocalDateTime): F[GenericTopicPartition[Option[OffsetAndTimestamp]]]

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

    def offsetsForTimes(ldt: LocalDateTime): F[GenericTopicPartition[Option[OffsetAndTimestamp]]] =
      for {
        tps <- partitionsFor
        ret <- kbc.ask.map {
          _.offsetsForTimes(tps.javaTimed(ldt)).asScala.toMap.mapValues(Option(_))
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

  def offsetRangeFor(
    start: LocalDateTime,
    end: LocalDateTime): F[GenericTopicPartition[KafkaOffsetRange]]

  def offsetRangeFor(end: LocalDateTime): F[GenericTopicPartition[KafkaOffsetRange]]

  def retrieveLastRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]

  def retrieveFirstRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]

  def retrieveRecordsForTimes(ldt: LocalDateTime): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]]
  def numOfRecords: F[GenericTopicPartition[Option[KafkaOffsetRange]]]
  def numOfRecordsSince(ldt: LocalDateTime): F[GenericTopicPartition[Option[KafkaOffsetRange]]]
}

object KafkaConsumerApi {

  def apply[F[_]: Concurrent, K, V](
    topicName: String,
    sharedConsumer: Eval[MVar[F, KafkaByteConsumer]]): KafkaConsumerApi[F, K, V] =
    new KafkaConsumerApiImpl[F, K, V](topicName, sharedConsumer)

  final private[this] class KafkaConsumerApiImpl[F[_]: Concurrent, K, V](
    topicName: String,
    sharedConsumer: Eval[MVar[F, KafkaByteConsumer]])
      extends KafkaConsumerApi[F, K, V] {
    import cats.mtl.implicits._
    private[this] val primitiveConsumer
      : KafkaPrimitiveConsumerApi[Kleisli[F, KafkaByteConsumer, *]] =
      KafkaPrimitiveConsumerApi[Kleisli[F, KafkaByteConsumer, *]](topicName)

    private[this] def atomically[A](r: Kleisli[F, KafkaByteConsumer, A]): F[A] =
      Sync[F].bracket(sharedConsumer.value.take)(r.run)(sharedConsumer.value.put)

    private def compuRange(
      from: GenericTopicPartition[Option[KafkaOffset]],
      to: GenericTopicPartition[Option[KafkaOffset]],
      end: GenericTopicPartition[Option[KafkaOffset]]): GenericTopicPartition[KafkaOffsetRange] =
      from
        .combineWith(to.combineWith(end)(_.orElse(_)))(
          (_, _).mapN((f, s) => KafkaOffsetRange(f, s)))
        .flatten

    override def offsetRangeFor(
      start: LocalDateTime,
      end: LocalDateTime): F[GenericTopicPartition[KafkaOffsetRange]] =
      atomically {
        for {
          from <- primitiveConsumer
            .offsetsForTimes(start)
            .map(_.mapValues(_.map(x => KafkaOffset(x.offset()))))
          to <- primitiveConsumer
            .offsetsForTimes(end)
            .map(_.mapValues(_.map(x => KafkaOffset(x.offset()))))
          endOffset <- primitiveConsumer.endOffsets
        } yield compuRange(from, to, endOffset)
      }

    override def offsetRangeFor(end: LocalDateTime): F[GenericTopicPartition[KafkaOffsetRange]] =
      atomically {
        for {
          from <- primitiveConsumer.beginningOffsets
          to <- primitiveConsumer
            .offsetsForTimes(end)
            .map(_.mapValues(_.map(x => KafkaOffset(x.offset()))))
          endOffset <- primitiveConsumer.endOffsets
        } yield compuRange(from, to, endOffset)
      }

    override def retrieveLastRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically {
        for {
          end <- primitiveConsumer.endOffsets
          rec <- end.value.toList.traverse {
            case (tp, of) =>
              of.filter(_.value > 0)
                .flatTraverse(x =>
                  primitiveConsumer.retrieveRecord(KafkaPartition(tp.partition), x.asLast))
          }
        } yield rec.flatten.sortBy(_.partition())
      }

    override def retrieveFirstRecords: F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically {
        for {
          beg <- primitiveConsumer.beginningOffsets
          rec <- beg.value.toList.traverse {
            case (tp, of) =>
              of.flatTraverse(x =>
                primitiveConsumer.retrieveRecord(KafkaPartition(tp.partition), x))
          }
        } yield rec.flatten.sortBy(_.partition())
      }

    override def retrieveRecordsForTimes(
      ldt: LocalDateTime): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically {
        for {
          oft <- primitiveConsumer.offsetsForTimes(ldt)
          rec <- oft.value.toList.traverse {
            case (tp, of) =>
              of.flatTraverse(
                x =>
                  primitiveConsumer
                    .retrieveRecord(KafkaPartition(tp.partition), KafkaOffset(x.offset())))
          }
        } yield rec.flatten
      }

    override def numOfRecords: F[GenericTopicPartition[Option[KafkaOffsetRange]]] =
      atomically {
        for {
          beg <- primitiveConsumer.beginningOffsets
          end <- primitiveConsumer.endOffsets
        } yield end.combineWith(beg)((_, _).mapN(KafkaOffsetRange(_, _)))
      }

    override def numOfRecordsSince(
      ldt: LocalDateTime): F[GenericTopicPartition[Option[KafkaOffsetRange]]] =
      atomically {
        for {
          oft <- primitiveConsumer
            .offsetsForTimes(ldt)
            .map(_.mapValues(_.map(x => KafkaOffset(x.offset()))))
          end <- primitiveConsumer.endOffsets
        } yield end.combineWith(oft)((_, _).mapN(KafkaOffsetRange(_, _)))
      }

    override def partitionsFor: F[ListOfTopicPartitions] =
      atomically(primitiveConsumer.partitionsFor)

    override def beginningOffsets: F[GenericTopicPartition[Option[KafkaOffset]]] =
      atomically(primitiveConsumer.beginningOffsets)

    override def endOffsets: F[GenericTopicPartition[Option[KafkaOffset]]] =
      atomically(primitiveConsumer.endOffsets)

    override def offsetsForTimes(
      time: LocalDateTime): F[GenericTopicPartition[Option[OffsetAndTimestamp]]] =
      atomically(primitiveConsumer.offsetsForTimes(time))

    override def retrieveRecord(
      partition: KafkaPartition,
      offset: KafkaOffset): F[Option[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      atomically(primitiveConsumer.retrieveRecord(partition, offset))
  }
}
