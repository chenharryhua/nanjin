package com.github.chenharryhua.nanjin.kafka
import cats.Show
import cats.effect.Concurrent
import cats.implicits._
import cats.tagless._
import com.github.chenharryhua.nanjin.codec.bitraverse._
import com.github.chenharryhua.nanjin.codec.iso._
import com.github.chenharryhua.nanjin.codec.show._
import fs2.kafka.AutoOffsetReset
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.util.Try

@autoFunctorK
@autoSemigroupalK
trait KafkaMonitoringApi[F[_], K, V] {
  def watch: F[Unit]
  def watchFromEarliest: F[Unit]

  def filter(pred: ConsumerRecord[Try[K], Try[V]]             => Boolean): F[Unit]
  def filterFromEarliest(pred: ConsumerRecord[Try[K], Try[V]] => Boolean): F[Unit]

  def badRecordsFromEarliest: F[Unit]
  def badRecords: F[Unit]

  def summaries: F[Unit]
}

object KafkaMonitoringApi {

  def apply[F[_]: Concurrent, K: Show, V: Show](
    topic: KafkaTopic[F, K, V]): KafkaMonitoringApi[F, K, V] =
    new KafkaTopicMonitoring[F, K, V](topic)

  final private class KafkaTopicMonitoring[F[_], K: Show, V: Show](topic: KafkaTopic[F, K, V])(
    implicit F: Concurrent[F])
      extends KafkaMonitoringApi[F, K, V] {

    private val fs2Channel: KafkaChannels.Fs2Channel[F, K, V] = topic.fs2Channel
    private val consumer: KafkaConsumerApi[F, K, V]           = topic.consumer

    private def watch(aor: AutoOffsetReset): F[Unit] =
      fs2Channel
        .updateConsumerSettings(_.withAutoOffsetReset(aor))
        .consume
        .map(m => topic.decoder(m).tryDecodeKeyValue)
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
        .map(m => topic.decoder(m).tryDecodeKeyValue)
        .filter(m => predict(isoFs2ComsumerRecord.get(m.record)))
        .map(_.show)
        .showLinesStdOut
        .compile
        .drain

    override def watch: F[Unit]             = watch(AutoOffsetReset.Latest)
    override def watchFromEarliest: F[Unit] = watch(AutoOffsetReset.Earliest)

    override def filter(pred: ConsumerRecord[Try[K], Try[V]] => Boolean): F[Unit] =
      filterWatch(pred, AutoOffsetReset.Latest)

    override def filterFromEarliest(pred: ConsumerRecord[Try[K], Try[V]] => Boolean): F[Unit] =
      filterWatch(pred, AutoOffsetReset.Earliest)

    override def badRecordsFromEarliest: F[Unit] =
      filterFromEarliest(cr => cr.key().isFailure || cr.value().isFailure)

    override def badRecords: F[Unit] =
      filter(cr => cr.key().isFailure || cr.value().isFailure)

    override def summaries: F[Unit] =
      for {
        num <- consumer.numOfRecords
        first <- consumer.retrieveFirstRecords.map(_.map(cr => topic.decoder(cr).tryDecodeKeyValue))
        last <- consumer.retrieveLastRecords.map(_.map(cr   => topic.decoder(cr).tryDecodeKeyValue))
      } yield println(s"""
                         |summaries:
                         |
                         |number of records: $num
                         |first records of each partitions: 
                         |${first.map(_.show).mkString("\n")}
                         |
                         |last records of each partitions:
                         |${last.map(_.show).mkString("\n")}
                         |""".stripMargin)
  }
}
