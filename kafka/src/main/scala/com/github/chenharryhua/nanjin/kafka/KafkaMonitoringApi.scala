package com.github.chenharryhua.nanjin.kafka

import java.nio.file.{Path, Paths}

import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.codec.iso
import com.github.chenharryhua.nanjin.kafka.common.KafkaOffset
import fs2.kafka.{produce, AutoOffsetReset, ProducerRecords}
import fs2.{text, Stream}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.Try

sealed trait KafkaMonitoringApi[F[_], K, V] {
  def watch: F[Unit]
  def watchFromEarliest: F[Unit]
  def watchFrom(njt: NJTimestamp): F[Unit]

  def filter(pred: ConsumerRecord[Try[K], Try[V]]             => Boolean): F[Unit]
  def filterFromEarliest(pred: ConsumerRecord[Try[K], Try[V]] => Boolean): F[Unit]

  def badRecordsFromEarliest: F[Unit]
  def badRecords: F[Unit]

  def summaries: F[Unit]

  def save: F[Unit]
  def replay: F[Unit]
}

object KafkaMonitoringApi {

  def apply[F[_]: ConcurrentEffect: ContextShift, K, V](
    topic: KafkaTopic[F, K, V]): KafkaMonitoringApi[F, K, V] =
    new KafkaTopicMonitoring[F, K, V](topic)

  final private class KafkaTopicMonitoring[F[_]: ContextShift, K, V](topic: KafkaTopic[F, K, V])(
    implicit F: ConcurrentEffect[F])
      extends KafkaMonitoringApi[F, K, V] {

    private val fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
      topic.fs2Channel.withConsumerSettings(_.withEnableAutoCommit(false))

    private def watch(aor: AutoOffsetReset): F[Unit] =
      fs2Channel
        .withConsumerSettings(_.withAutoOffsetReset(aor))
        .consume
        .map { m =>
          val rec: GenericRecord = topic.description.toAvro(m)
          rec.toString
        }
        .showLinesStdOut
        .compile
        .drain

    private def filterWatch(
      predict: ConsumerRecord[Try[K], Try[V]] => Boolean,
      aor: AutoOffsetReset): F[Unit] =
      fs2Channel
        .withConsumerSettings(_.withAutoOffsetReset(aor))
        .consume
        .map(m => topic.decoder(m).tryDecodeKeyValue)
        .filter(m => predict(iso.isoFs2ComsumerRecord.get(m.record)))
        .map(_.toString)
        .showLinesStdOut
        .compile
        .drain

    override def watchFrom(njt: NJTimestamp): F[Unit] =
      for {
        gtp <- KafkaConsumerApi(topic.description).use { c =>
          for {
            os <- c.offsetsForTimes(njt)
            e <- c.endOffsets
          } yield os.combineWith(e)(_.orElse(_))
        }
        _ <- fs2Channel
          .assign(gtp.flatten[KafkaOffset].mapValues(_.value).value)
          .map(m => topic.decoder(m).tryDecodeKeyValue)
          .map(_.toString)
          .showLinesStdOut
          .compile
          .drain
      } yield ()

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
      KafkaConsumerApi(topic.description).use { consumer =>
        for {
          num <- consumer.numOfRecords
          first <- consumer.retrieveFirstRecords.map(_.map(cr =>
            topic.decoder(cr).tryDecodeKeyValue))
          last <- consumer.retrieveLastRecords.map(_.map(cr => topic.decoder(cr).tryDecodeKeyValue))
        } yield println(s"""
                           |summaries:
                           |
                           |number of records: $num
                           |first records of each partitions: 
                           |${first.map(_.toString).mkString("\n")}
                           |
                           |last records of each partitions:
                           |${last.map(_.toString).mkString("\n")}
                           |""".stripMargin)
      }

    private val path: Path = Paths.get(s"./data/kafka/json/${topic.topicName}.json")

    override def save: F[Unit] =
      Stream
        .resource[F, Blocker](Blocker[F])
        .flatMap { blocker =>
          fs2Channel.consume
            .map(x => topic.description.toJson(x).noSpaces)
            .intersperse("\n")
            .through(text.utf8Encode)
            .through(fs2.io.file.writeAll(path, blocker))
        }
        .compile
        .drain

    def replay: F[Unit] =
      Stream
        .resource(Blocker[F])
        .flatMap { blocker =>
          fs2.io.file
            .readAll(path, blocker, 5000)
            .through(fs2.text.utf8Decode)
            .through(fs2.text.lines)
            .mapFilter { str =>
              topic.description
                .fromJsonStr(str)
                .leftMap(err => println(s"decode json error: ${err.getMessage}"))
                .toOption
            }
            .map { nj =>
              ProducerRecords.one(
                iso.isoFs2ProducerRecord[K, V].reverseGet(nj.toNJProducerRecord.toProducerRecord))
            }
            .through(produce(topic.description.fs2ProducerSettings))
        }
        .compile
        .drain
  }
}
