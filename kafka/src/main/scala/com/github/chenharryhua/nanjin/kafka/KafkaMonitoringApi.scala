package com.github.chenharryhua.nanjin.kafka

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.codec.NJConsumerMessage._
import com.github.chenharryhua.nanjin.kafka.codec.iso
import com.github.chenharryhua.nanjin.kafka.common.{KafkaOffset, NJConsumerRecord}
import com.github.chenharryhua.nanjin.pipes.{GenericRecordEncoder, JsonAvroSerialization}
import com.github.chenharryhua.nanjin.utils.Keyboard
import fs2.Stream
import fs2.kafka.{produce, AutoOffsetReset, ProducerRecord, ProducerRecords}
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

  def apply[F[_]: ConcurrentEffect: Timer: ContextShift, K, V](
    topic: KafkaTopic[F, K, V]): KafkaMonitoringApi[F, K, V] =
    new KafkaTopicMonitoring[F, K, V](topic)

  final private class KafkaTopicMonitoring[F[_]: ContextShift: Timer, K, V](
    topic: KafkaTopic[F, K, V])(implicit F: ConcurrentEffect[F])
      extends KafkaMonitoringApi[F, K, V] {

    import topic.topicDef.{avroKeyDecoder, avroKeyEncoder, avroValDecoder, avroValEncoder}

    private val fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
      topic.fs2Channel.withConsumerSettings(_.withEnableAutoCommit(false))

    private def watch(aor: AutoOffsetReset): F[Unit] =
      Blocker[F].use { blocker =>
        val pipe = new JsonAvroSerialization[F](topic.topicDef.schemaFor.schema)
        val gr   = new GenericRecordEncoder[F, NJConsumerRecord[K, V]]()
        Keyboard.signal.flatMap { signal =>
          fs2Channel
            .withConsumerSettings(_.withAutoOffsetReset(aor))
            .stream
            .map { m =>
              val (err, r) = topic.decoder(m).logRecord.run
              err.map(e => pprint.pprintln(e))
              r
            }
            .through(gr.encode)
            .through(pipe.prettyJson)
            .showLinesStdOut
            .pauseWhen(signal.map(_.contains(Keyboard.pauSe)))
            .interruptWhen(signal.map(_.contains(Keyboard.Quit)))
        }.compile.drain
      }

    private def filterWatch(
      predict: ConsumerRecord[Try[K], Try[V]] => Boolean,
      aor: AutoOffsetReset): F[Unit] =
      Blocker[F].use { blocker =>
        val pipe = new JsonAvroSerialization[F](topic.topicDef.schemaFor.schema)
        val gr   = new GenericRecordEncoder[F, NJConsumerRecord[K, V]]()
        Keyboard.signal.flatMap { signal =>
          fs2Channel
            .withConsumerSettings(_.withAutoOffsetReset(aor))
            .stream
            .filter(m =>
              predict(iso.isoFs2ComsumerRecord.get(topic.decoder(m).tryDecodeKeyValue.record)))
            .map(m => topic.decoder(m).record)
            .through(gr.encode)
            .through(pipe.prettyJson)
            .showLinesStdOut
            .pauseWhen(signal.map(_.contains(Keyboard.pauSe)))
            .interruptWhen(signal.map(_.contains(Keyboard.Quit)))
        }.compile.drain
      }

    override def watchFrom(njt: NJTimestamp): F[Unit] = {
      val run: Stream[F, Unit] = for {
        blocker <- Stream.resource(Blocker[F])
        pipe = new JsonAvroSerialization[F](topic.topicDef.schemaFor.schema)
        gr   = new GenericRecordEncoder[F, NJConsumerRecord[K, V]]()
        kcs <- Stream.resource(topic.shortLiveConsumer)
        gtp <- Stream.eval(for {
          os <- kcs.offsetsForTimes(njt)
          e <- kcs.endOffsets
        } yield os.combineWith(e)(_.orElse(_)))
        signal <- Keyboard.signal
        _ <-
          fs2Channel
            .assign(gtp.flatten[KafkaOffset].mapValues(_.value).value)
            .map(m => topic.decoder(m).record)
            .through(gr.encode)
            .through(pipe.prettyJson)
            .showLinesStdOut
            .pauseWhen(signal.map(_.contains(Keyboard.pauSe)))
            .interruptWhen(signal.map(_.contains(Keyboard.Quit)))
      } yield ()
      run.compile.drain
    }

    override def watchFrom(njt: String): F[Unit] =
      watchFrom(NJTimestamp(njt))

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
        signal <- Keyboard.signal
        _ <- fs2Channel.stream.map { m =>
          val cr = other.decoder(m).nullableDecode.record
          val ts = cr.timestamp.createTime.orElse(
            cr.timestamp.logAppendTime.orElse(cr.timestamp.unknownTime))
          val pr = ProducerRecord(other.topicName.value, cr.key, cr.value)
            .withHeaders(cr.headers)
            .withPartition(cr.partition)
          ProducerRecords.one(ts.fold(pr)(pr.withTimestamp))
        }.through(produce[F, K, V, Unit](other.fs2Channel.producerSettings))
          .pauseWhen(signal.map(_.contains(Keyboard.pauSe)))
          .interruptWhen(signal.map(_.contains(Keyboard.Quit)))
      } yield ()
      run.chunkN(10000).map(_ => print(".")).compile.drain
    }
  }
}
