package com.github.chenharryhua.nanjin.kafka
import java.nio.file.Paths
import java.time.LocalDateTime

import akka.stream.scaladsl.FileIO
import akka.util.ByteString
import cats.effect.{Async, Concurrent, ContextShift, Resource}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Signal
import fs2.kafka.AutoOffsetReset
import io.circe.Encoder
import io.circe.syntax._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.jline.terminal.{Terminal, TerminalBuilder}

trait KafkaMonitoringApi[F[_], K, V] {
  def watchFromLatest: F[Unit]
  def watchFromEarliest: F[Unit]
  def filterFromLatest(pred: ConsumerRecord[K, V]   => Boolean): F[Unit]
  def filterFromEarliest(pred: ConsumerRecord[K, V] => Boolean): F[Unit]
  def summaries: F[Unit]

  def saveJson(start: LocalDateTime, end: LocalDateTime, path: String)(
    implicit ev: Encoder[V]): F[Long]
}

object KafkaMonitoringApi {

  def apply[F[_]: Concurrent: ContextShift, K, V](
    fs2Channel: KafkaChannels.Fs2Channel[F, K, V],
    akkaResource: Resource[F, KafkaChannels.AkkaChannel[F, K, V]],
    consumer: KafkaConsumerApi[F, K, V]
  ): KafkaMonitoringApi[F, K, V] =
    new KafkaTopicMonitoring[F, K, V](fs2Channel, akkaResource, consumer) {}

  private[kafka] abstract class KafkaTopicMonitoring[F[_]: ContextShift, K, V](
    fs2Channel: KafkaChannels.Fs2Channel[F, K, V],
    akkaResource: Resource[F, KafkaChannels.AkkaChannel[F, K, V]],
    consumer: KafkaConsumerApi[F, K, V])(implicit F: Concurrent[F])
      extends KafkaMonitoringApi[F, K, V] with ShowKafkaMessage with BitraverseFs2Message {

    private val PAUSE: Char    = 's'
    private val QUIT: Char     = 'q'
    private val CONTINUE: Char = 'c'

    private def keyboardSignal: Stream[F, Signal[F, Option[Char]]] =
      Stream
        .bracket(F.delay {
          val terminal: Terminal = TerminalBuilder.builder().jna(true).system(true).build()
          terminal.enterRawMode
          (terminal, terminal.reader)
        }) {
          case (terminal, reader) =>
            F.delay {
              terminal.close()
              reader.close()
            }
        }
        .flatMap { case (_, r) => Stream.repeatEval(F.delay(r.read().toChar)) }
        .noneTerminate
        .hold(Some(CONTINUE))

    private def watch(aor: AutoOffsetReset): F[Unit] =
      keyboardSignal.flatMap { signal =>
        fs2Channel
          .updateConsumerSettings(_.withAutoOffsetReset(aor))
          .consume
          .map(fs2Channel.safeDecodeKeyValue)
          .map(_.show)
          .showLinesStdOut
          .pauseWhen(signal.map(_.contains(PAUSE)))
          .interruptWhen(signal.map(_.contains(QUIT)))
      }.compile.drain

    private def filterWatch(predict: ConsumerRecord[K, V] => Boolean, aor: AutoOffsetReset)(
      implicit F: Concurrent[F]): F[Unit] =
      keyboardSignal.flatMap { signal =>
        fs2Channel
          .updateConsumerSettings(_.withAutoOffsetReset(aor))
          .consume
          .map(fs2Channel.decode)
          .filter(m => predict(isoFs2ComsumerRecord.get(m.record)))
          .map(_.show)
          .showLinesStdOut
          .pauseWhen(signal.map(_.contains(PAUSE)))
          .interruptWhen(signal.map(_.contains(QUIT)))
      }.compile.drain

    override def watchFromLatest: F[Unit] =
      watch(AutoOffsetReset.Latest)

    override def watchFromEarliest: F[Unit] =
      watch(AutoOffsetReset.Earliest)

    override def filterFromLatest(pred: ConsumerRecord[K, V] => Boolean): F[Unit] =
      filterWatch(pred, AutoOffsetReset.Latest)

    override def filterFromEarliest(pred: ConsumerRecord[K, V] => Boolean): F[Unit] =
      filterWatch(pred, AutoOffsetReset.Earliest)

    override def summaries: F[Unit] =
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

    override def saveJson(start: LocalDateTime, end: LocalDateTime, path: String)(
      implicit ev: Encoder[V]): F[Long] =
      for {
        range <- consumer.offsetRangeFor(start, end)
        beginning = range.mapValues(_.fromOffset)
        ending    = range.mapValues(_.untilOffset)
        size      = range.value.map { case (_, v) => v.size }.sum
        _ <- akkaResource.use { chn =>
          Async.fromFuture(
            F.pure(
              chn
                .assign(beginning.value)
                .takeWhile(p => ending.get(p.topic, p.partition).exists(p.offset < _))
                .take(size)
                .map(m => chn.decode(m).value.asJson.noSpaces)
                .intersperse("\n")
                .map(ByteString(_))
                .runWith(FileIO.toPath(Paths.get(path)))(chn.materializer)))
        }
      } yield size
  }
}
