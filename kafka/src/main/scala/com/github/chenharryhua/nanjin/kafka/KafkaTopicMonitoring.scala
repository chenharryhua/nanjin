package com.github.chenharryhua.nanjin.kafka
import cats.effect.Concurrent
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Signal
import fs2.kafka.AutoOffsetReset
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.jline.terminal.{Terminal, TerminalBuilder}

trait KafkaTopicMonitoring[F[_], K, V] extends ShowKafkaMessage with BitraverseFs2Message {
  protected def fs2Channel: KafkaChannels.Fs2Channel[F, K, V]
  protected def consumer: KafkaConsumerApi[F, K, V]
  implicit def F: Concurrent[F]

  private val keyboardSignal: Stream[F, Signal[F, Option[Char]]] =
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

  private def watch(aor: AutoOffsetReset): F[Unit] =
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

  private def filterWatch(predict: ConsumerRecord[K, V] => Boolean, aor: AutoOffsetReset): F[Unit] =
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
