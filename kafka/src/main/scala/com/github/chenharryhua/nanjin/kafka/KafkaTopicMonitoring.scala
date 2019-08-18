package com.github.chenharryhua.nanjin.kafka
import java.nio.file.Paths
import java.time.LocalDateTime

import akka.stream.scaladsl.FileIO
import akka.util.ByteString
import cats.effect.{Async, Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Signal
import fs2.kafka.AutoOffsetReset
import io.circe.Encoder
import io.circe.syntax._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.jline.terminal.{Terminal, TerminalBuilder}

trait KafkaTopicMonitoring[F[_], K, V] extends ShowKafkaMessage with BitraverseFs2Message {
  protected def fs2Channel: KafkaChannels.Fs2Channel[F, K, V]
  protected def akkaResource: Resource[F, KafkaChannels.AkkaChannel[F, K, V]]
  protected def consumer: KafkaConsumerApi[F, K, V]

  private def keyboardSignal(implicit F: Concurrent[F]): Stream[F, Signal[F, Option[Char]]] =
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
      .hold(Some('c'))

  private def watch(aor: AutoOffsetReset)(implicit F: Concurrent[F]): F[Unit] =
    keyboardSignal.flatMap { signal =>
      fs2Channel
        .updateConsumerSettings(_.withAutoOffsetReset(aor))
        .consume
        .map(fs2Channel.safeDecodeKeyValue)
        .map(_.show)
        .showLinesStdOut
        .pauseWhen(signal.map(_.contains('s')))
        .interruptWhen(signal.map(_.contains('q')))
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
        .pauseWhen(signal.map(_.contains('s')))
        .interruptWhen(signal.map(_.contains('q')))
    }.compile.drain

  def watchFromLatest(implicit F: Concurrent[F]): F[Unit] =
    watch(AutoOffsetReset.Latest)

  def watchFromEarliest(implicit F: Concurrent[F]): F[Unit] =
    watch(AutoOffsetReset.Earliest)

  def filterFromLatest(pred: ConsumerRecord[K, V] => Boolean)(implicit F: Concurrent[F]): F[Unit] =
    filterWatch(pred, AutoOffsetReset.Latest)

  def filterFromEarliest(pred: ConsumerRecord[K, V] => Boolean)(
    implicit F: Concurrent[F]): F[Unit] =
    filterWatch(pred, AutoOffsetReset.Earliest)

  def summaries(implicit ev: Sync[F]): F[Unit] =
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

  def saveJson(start: LocalDateTime, end: LocalDateTime, path: String)(
    implicit ev: Encoder[V],
    ev2: ContextShift[F],
    F: Concurrent[F]): F[Unit] =
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
    } yield ()
}
