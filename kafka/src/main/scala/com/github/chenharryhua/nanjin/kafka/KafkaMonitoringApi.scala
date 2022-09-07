package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Console
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJConsumerRecordWithError}
import com.sksamuel.avro4s.AvroOutputStream
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, ProducerRecord, ProducerRecords}
import org.typelevel.cats.time.instances.localtime
import java.io.ByteArrayOutputStream

sealed trait KafkaMonitoringApi[F[_], K, V] {
  def watch: F[Unit]
  def watchFromEarliest: F[Unit]
  def watchFrom(njt: NJTimestamp): F[Unit]
  def watchFrom(njt: String): F[Unit]

  def watchFilter(f: NJConsumerRecordWithError[K, V] => Boolean): F[Unit]

  def summaries: F[Unit]

  def carbonCopyTo(other: KafkaTopic[F, K, V]): F[Unit]
}

object KafkaMonitoringApi {

  def apply[F[_]: Async: Console, K, V](topic: KafkaTopic[F, K, V]): KafkaMonitoringApi[F, K, V] =
    new KafkaTopicMonitoring[F, K, V](topic)

  final private class KafkaTopicMonitoring[F[_]: Async, K, V](topic: KafkaTopic[F, K, V])(implicit
    C: Console[F])
      extends KafkaMonitoringApi[F, K, V] with localtime {

    private def fetchData(aor: AutoOffsetReset): Stream[F, NJConsumerRecordWithError[K, V]] =
      topic.consume
        .updateConfig(_.withAutoOffsetReset(aor).withEnableAutoCommit(false))
        .stream
        .map(m => topic.decode(m))

    private val printJackson: (NJConsumerRecordWithError[K, V], Long) => F[Unit] =
      (cr: NJConsumerRecordWithError[K, V], index: Long) =>
        Resource.fromAutoCloseable[F, ByteArrayOutputStream](Async[F].pure(new ByteArrayOutputStream)).use {
          bos =>
            for {
              _ <- C.println(
                s"timestamp: ${cr.metaInfo(topic.context.settings.zoneId).timestamp.toLocalTime.show}")
              _ <- cr.key.leftTraverse(C.println)
              _ <- cr.value.leftTraverse(C.println)
              _ <- C.println {
                val aos: AvroOutputStream[NJConsumerRecord[K, V]] = AvroOutputStream
                  .json[NJConsumerRecord[K, V]](
                    NJConsumerRecord
                      .avroCodec(topic.codec.keySerde.avroCodec, topic.codec.valSerde.avroCodec)
                      .avroEncoder)
                  .to(bos)
                  .build()
                aos.write(cr.toNJConsumerRecord)
                aos.close()
                bos.flush()
                bos.toString()
              }
              _ <- C.println(f"$index%20d-------------------".replace(" ", "-"))
            } yield ()
        }

    override def watchFrom(njt: NJTimestamp): F[Unit] = {
      val run: Stream[F, Unit] = for {
        kcs <- Stream.resource(topic.shortLiveConsumer)
        gtp <- Stream.eval(for {
          os <- kcs.offsetsForTimes(njt)
          e <- kcs.endOffsets
        } yield os.combineWith(e)(_.orElse(_)))
        _ <- topic.consume
          .updateConfig(_.withEnableAutoCommit(false))
          .assign(gtp.mapValues(_.getOrElse(KafkaOffset(0))))
          .map(m => topic.decode(m))
          .zipWithIndex
          .evalMap(printJackson.tupled)
      } yield ()
      run.compile.drain
    }

    override def watchFrom(njt: String): F[Unit] =
      watchFrom(NJTimestamp(njt, topic.context.settings.zoneId))

    override def watch: F[Unit] =
      fetchData(AutoOffsetReset.Latest).zipWithIndex.evalMap(printJackson.tupled).compile.drain

    override def watchFilter(f: NJConsumerRecordWithError[K, V] => Boolean): F[Unit] =
      fetchData(AutoOffsetReset.Latest).filter(f).zipWithIndex.evalMap(printJackson.tupled).compile.drain

    override def watchFromEarliest: F[Unit] =
      fetchData(AutoOffsetReset.Earliest).zipWithIndex.evalMap(printJackson.tupled).compile.drain

    override def summaries: F[Unit] =
      topic.shortLiveConsumer.use { consumer =>
        for {
          num <- consumer.offsetRangeForAll
          first <- consumer.retrieveFirstRecords.map(_.map(cr => topic.decoder(cr).tryDecodeKeyValue))
          last <- consumer.retrieveLastRecords.map(_.map(cr => topic.decoder(cr).tryDecodeKeyValue))
          _ <- C.println(s"""
                            |summaries:
                            |
                            |number of records: $num
                            |first records of each partitions: 
                            |${first.map(_.toString).mkString("\n")}
                            |
                            |last records of each partitions:
                            |${last.map(_.toString).mkString("\n")}
                            |""".stripMargin)
        } yield ()
      }

    override def carbonCopyTo(other: KafkaTopic[F, K, V]): F[Unit] = {
      val run = for {
        _ <- topic.consume
          .updateConfig(_.withEnableAutoCommit(false))
          .stream
          .map { m =>
            val cr = other.decoder(m).nullableDecode.record
            val ts =
              cr.timestamp.createTime.orElse(cr.timestamp.logAppendTime.orElse(cr.timestamp.unknownTime))
            val pr =
              ProducerRecord(other.topicName.value, cr.key, cr.value)
                .withHeaders(cr.headers)
                .withPartition(cr.partition)
            ProducerRecords.one(ts.fold(pr)(pr.withTimestamp))
          }
          .through(other.produce.pipe)
      } yield ()
      run.chunkN(1000).map(_ => C.print(".")).compile.drain
    }
  }
}
