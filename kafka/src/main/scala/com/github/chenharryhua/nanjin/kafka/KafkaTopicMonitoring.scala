package com.github.chenharryhua.nanjin.kafka
import cats.effect.Sync
import cats.implicits._
import fs2.kafka.AutoOffsetReset
import org.apache.kafka.clients.consumer.ConsumerRecord

trait KafkaTopicMonitoring[F[_], K, V] extends ShowKafkaMessage with BitraverseFs2Message {
  protected def fs2Channel: KafkaChannels.Fs2Channel[F, K, V]
  protected def consumer: KafkaConsumerApi[F, K, V]
  implicit def syncF: Sync[F]

  private def watch(aor: AutoOffsetReset): F[Unit] =
    fs2Channel
      .updateConsumerSettings(_.withAutoOffsetReset(aor))
      .consume
      .map(fs2Channel.safeDecodeKeyValue)
      .map(_.show)
      .showLinesStdOut
      .compile
      .drain

  private def filterWatch(predict: ConsumerRecord[K, V] => Boolean, aor: AutoOffsetReset): F[Unit] =
    fs2Channel
      .updateConsumerSettings(_.withAutoOffsetReset(aor))
      .consume
      .map(fs2Channel.decode)
      .filter(m => predict(isoFs2ComsumerRecord.get(m.record)))
      .map(_.show)
      .showLinesStdOut
      .compile
      .drain

  def watchFromLatest: F[Unit] =
    watch(AutoOffsetReset.Latest)

  def watchFromEarliest: F[Unit] =
    watch(AutoOffsetReset.Earliest)

  def filterFromLatest(pred: ConsumerRecord[K, V] => Boolean): F[Unit] =
    filterWatch(pred, AutoOffsetReset.Latest)

  def filterFromEarliest(pred: ConsumerRecord[K, V] => Boolean): F[Unit] =
    filterWatch(pred, AutoOffsetReset.Earliest)

  def summaries: F[Unit] =
    for {
      num <- consumer.numOfRecords
      first <- consumer.retrieveFirstRecords
      last <- consumer.retrieveLastRecords
    } yield println(s"""
                       |summaries:
                       |
                       |${num.show}
                       |first records: 
                       |${first.map(_.show).mkString("\n")}
                       |
                       |last records:
                       |${last.map(_.show).mkString("\n")}
                       |""".stripMargin)

}
