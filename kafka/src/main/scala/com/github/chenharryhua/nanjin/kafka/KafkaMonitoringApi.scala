package com.github.chenharryhua.nanjin.kafka

import java.io.{FileOutputStream, OutputStream}
import java.nio.file.Paths
import java.time.ZonedDateTime

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource}
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.codec.NJConsumerMessage._
import com.github.chenharryhua.nanjin.kafka.codec.iso
import com.github.chenharryhua.nanjin.kafka.common.KafkaOffset
import fs2.Stream
import fs2.kafka.{produce, AutoOffsetReset, ProducerRecord, ProducerRecords}
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

  def saveJackson: F[Unit]
  def saveAvro: F[Unit]
  def replayJackson: F[Unit]
  def carbonCopyTo(other: KafkaTopicKit[K, V]): F[Unit]
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
          val (err, r) = topic.kit.decoder(m).logRecord.run
          err.map(e => pprint.pprintln(e))
          s"""|local now: ${ZonedDateTime.now}
              |timestamp: ${NJTimestamp(r.timestamp).local}
              |${topic.kit.topicDef.toJackson(r).spaces2}""".stripMargin
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
        .filter(m =>
          predict(iso.isoFs2ComsumerRecord.get(topic.decoder(m).tryDecodeKeyValue.record)))
        .map(m => topic.kit.toJackson(m).spaces2)
        .showLinesStdOut
        .compile
        .drain

    override def watchFrom(njt: NJTimestamp): F[Unit] =
      for {
        gtp <- KafkaConsumerApi(topic.kit).use { c =>
          for {
            os <- c.offsetsForTimes(njt)
            e <- c.endOffsets
          } yield os.combineWith(e)(_.orElse(_))
        }
        _ <- fs2Channel
          .assign(gtp.flatten[KafkaOffset].mapValues(_.value).value)
          .map(m => topic.kit.toJackson(m).spaces2)
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
      KafkaConsumerApi(topic.kit).use { consumer =>
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

    private val path: String = s"./data/kafka/monitor/${topic.topicName}"

    private def outFile(path: String): Resource[F, OutputStream] =
      Resource.make[F, OutputStream](F.pure(new FileOutputStream(path)))(os =>
        F.delay { os.flush(); os.close() })

    override def saveJackson: F[Unit] = {
      val run = for {
        blocker <- Stream.resource[F, Blocker](Blocker[F])
        os <- Stream.resource(outFile(path + ".json"))
        data <- fs2Channel.consume
          .map(m => topic.kit.decoder(m).record)
          .through(topic.kit.topicDef.jacksonSink[F](os))
      } yield ()
      run.compile.drain
    }

    override def saveAvro: F[Unit] = {
      val run = for {
        blocker <- Stream.resource[F, Blocker](Blocker[F])
        os <- Stream.resource(outFile(path + ".avro"))
        data <- fs2Channel.consume
          .map(m => topic.kit.decoder(m).record)
          .through(topic.kit.topicDef.avroSink[F](os))
      } yield ()
      run.compile.drain
    }

    override def replayJackson: F[Unit] =
      Stream
        .resource(Blocker[F])
        .flatMap { blocker =>
          fs2.io.file
            .readAll(Paths.get(path + ".json"), blocker, 5000)
            .through(fs2.text.utf8Decode)
            .through(fs2.text.lines)
            .mapFilter { str =>
              topic.kit
                .fromJackson(str)
                .toEither
                .leftMap(e => println(s"${e.getMessage}. source: $str"))
                .toOption
            }
            .chunks
            .map(chk =>
              ProducerRecords(chk.map(_.toNJProducerRecord.toFs2ProducerRecord(topic.topicName))))
            .through(produce(topic.kit.fs2ProducerSettings[F]))
            .map(_ => print("."))
        }
        .compile
        .drain

    override def carbonCopyTo(other: KafkaTopicKit[K, V]): F[Unit] =
      fs2Channel.consume.map { m =>
        val cr = other.decoder(m).nullableDecode.record
        val ts = cr.timestamp.createTime.orElse(
          cr.timestamp.logAppendTime.orElse(cr.timestamp.unknownTime))
        val pr = ProducerRecord(other.topicName.value, cr.key, cr.value)
          .withHeaders(cr.headers)
          .withPartition(cr.partition)
        ProducerRecords.one(ts.fold(pr)(pr.withTimestamp))
      }.through(produce(other.fs2ProducerSettings))
        .chunks
        .map(_ => print("."))
        .compile
        .drain
  }
}
