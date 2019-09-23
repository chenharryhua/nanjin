package com.github.chenharryhua.nanjin.kafka
import cats.Show
import cats.effect.Concurrent
import cats.implicits._
import cats.tagless._
import com.github.chenharryhua.nanjin.codec._
import fs2.kafka.AutoOffsetReset
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.Try

@autoFunctorK
@autoSemigroupalK
trait KafkaMonitoringApi[F[_], K, V] {
  def watchFromLatest: F[Unit]
  def watchFromEarliest: F[Unit]

  def filterFromLatest(pred: ConsumerRecord[Try[K], Try[V]]   => Boolean): F[Unit]
  def filterFromEarliest(pred: ConsumerRecord[Try[K], Try[V]] => Boolean): F[Unit]

  def badRecordsFromEarliest: F[Unit]
  def badRecordsFromLatest: F[Unit]

  def summaries: F[Unit]
}

object KafkaMonitoringApi {

  def apply[F[_]: Concurrent, K: Show, V: Show](
    fs2Channel: KafkaChannels.Fs2Channel[F, K, V],
    consumer: KafkaConsumerApi[F, K, V]
  ): KafkaMonitoringApi[F, K, V] =
    new KafkaTopicMonitoring[F, K, V](fs2Channel, consumer)

  final private class KafkaTopicMonitoring[F[_], K: Show, V: Show](
    fs2Channel: KafkaChannels.Fs2Channel[F, K, V],
    consumer: KafkaConsumerApi[F, K, V])(implicit F: Concurrent[F])
      extends KafkaMonitoringApi[F, K, V] {

    private def watch(aor: AutoOffsetReset): F[Unit] =
      fs2Channel
        .updateConsumerSettings(_.withAutoOffsetReset(aor))
        .consume
        .map(fs2Channel.messageDecoder.tryDecodeKeyValue)
        .map(_.show)
        .showLinesStdOut
        .compile
        .drain

    private def filterWatch(
      predict: ConsumerRecord[Try[K], Try[V]] => Boolean,
      aor: AutoOffsetReset): F[Unit] =
      fs2Channel
        .updateConsumerSettings(_.withAutoOffsetReset(aor))
        .consume
        .map(fs2Channel.messageDecoder.tryDecodeKeyValue)
        .filter(m => predict(isoFs2ComsumerRecord.get(m.record)))
        .map(_.show)
        .showLinesStdOut
        .compile
        .drain

    override def watchFromLatest: F[Unit]   = watch(AutoOffsetReset.Latest)
    override def watchFromEarliest: F[Unit] = watch(AutoOffsetReset.Earliest)

    override def filterFromLatest(pred: ConsumerRecord[Try[K], Try[V]] => Boolean): F[Unit] =
      filterWatch(pred, AutoOffsetReset.Latest)

    override def filterFromEarliest(pred: ConsumerRecord[Try[K], Try[V]] => Boolean): F[Unit] =
      filterWatch(pred, AutoOffsetReset.Earliest)

    override def badRecordsFromEarliest: F[Unit] =
      filterFromEarliest(cr => cr.key().isFailure || cr.value().isFailure)
    override def badRecordsFromLatest: F[Unit] =
      filterFromLatest(cr => cr.key().isFailure || cr.value().isFailure)

    override def summaries: F[Unit] =
      for {
        num <- consumer.numOfRecords
        first <- consumer.retrieveFirstRecords.map(_.map(cr =>
          fs2Channel.recordDecoder.tryDecodeKeyValue(cr)))
        last <- consumer.retrieveLastRecords.map(_.map(cr =>
          fs2Channel.recordDecoder.tryDecodeKeyValue(cr)))
      } yield println(s"""
                         |summaries:
                         |
                         |number of records: ${num.show}
                         |first records of each partitions: 
                         |${first.map(_.show).mkString("\n")}
                         |
                         |last records of each partitions:
                         |${last.map(_.show).mkString("\n")}
                         |""".stripMargin)

  }
}
