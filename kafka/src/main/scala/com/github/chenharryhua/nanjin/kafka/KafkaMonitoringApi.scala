package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.messages.kafka.*
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, ProducerRecord, ProducerRecords}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.Try

sealed trait KafkaMonitoringApi[F[_], K, V] {
  def watch: F[Unit]
  def watchFromEarliest: F[Unit]
  def watchFrom(njt: NJTimestamp): F[Unit]
  def watchFrom(njt: String): F[Unit]

  def filter(pred: ConsumerRecord[Try[K], Try[V]] => Boolean): F[Unit]
  def filterFromEarliest(pred: ConsumerRecord[Try[K], Try[V]] => Boolean): F[Unit]

  def badRecordsFromEarliest: F[Unit]
  def badRecords: F[Unit]

  def summaries: F[Unit]

  def carbonCopyTo(other: KafkaTopic[F, K, V]): F[Unit]
}

object KafkaMonitoringApi {

  def apply[F[_]: Async, K, V](topic: KafkaTopic[F, K, V]): KafkaMonitoringApi[F, K, V] =
    new KafkaTopicMonitoring[F, K, V](topic)

  final private class KafkaTopicMonitoring[F[_]: Async, K, V](topic: KafkaTopic[F, K, V])
      extends KafkaMonitoringApi[F, K, V] {

    private def watch(aor: AutoOffsetReset): F[Unit] =
      topic.fs2Channel
        .updateConsumer(_.withAutoOffsetReset(aor))
        .stream
        .map(m => topic.decoder(m).tryDecodeKeyValue.toString)
        .debug()
        .compile
        .drain

    private def filterWatch(predict: ConsumerRecord[Try[K], Try[V]] => Boolean, aor: AutoOffsetReset): F[Unit] =
      topic.fs2Channel
        .updateConsumer(_.withAutoOffsetReset(aor))
        .stream
        .filter(m => predict(isoFs2ComsumerRecord.get(topic.decoder(m).tryDecodeKeyValue.record)))
        .map(m => topic.decoder(m).tryDecodeKeyValue.toString)
        .debug()
        .compile
        .drain

    override def watchFrom(njt: NJTimestamp): F[Unit] = {
      val run: Stream[F, Unit] = for {
        kcs <- Stream.resource(topic.shortLiveConsumer)
        gtp <- Stream.eval(for {
          os <- kcs.offsetsForTimes(njt)
          e <- kcs.endOffsets
        } yield os.combineWith(e)(_.orElse(_)))
        _ <- topic.fs2Channel
          .assign(gtp.mapValues(_.getOrElse(KafkaOffset(0))))
          .map(m => topic.decoder(m).tryDecodeKeyValue.toString)
          .debug()
      } yield ()
      run.compile.drain
    }

    override def watchFrom(njt: String): F[Unit] = watchFrom(NJTimestamp(njt))

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
      topic.shortLiveConsumer.use { consumer =>
        for {
          num <- consumer.numOfRecords
          first <-
            consumer.retrieveFirstRecords.map(_.map(cr => topic.decoder(cr).tryDecodeKeyValue))
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

    override def carbonCopyTo(other: KafkaTopic[F, K, V]): F[Unit] = {
      val run = for {
        _ <- topic.fs2Channel.stream.map { m =>
          val cr = other.decoder(m).nullableDecode.record
          val ts = cr.timestamp.createTime.orElse(cr.timestamp.logAppendTime.orElse(cr.timestamp.unknownTime))
          val pr =
            ProducerRecord(other.topicName.value, cr.key, cr.value).withHeaders(cr.headers).withPartition(cr.partition)
          ProducerRecords.one(ts.fold(pr)(pr.withTimestamp))
        }.through(other.fs2Channel.producerPipe)
      } yield ()
      run.chunkN(10000).map(_ => print(".")).compile.drain
    }
  }
}
