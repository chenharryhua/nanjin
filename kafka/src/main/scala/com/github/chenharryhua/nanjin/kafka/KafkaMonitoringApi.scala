package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Console
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJConsumerRecordWithError}
import com.sksamuel.avro4s.AvroOutputStream
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, ProducerRecord, ProducerRecords}
import org.typelevel.cats.time.instances.zoneddatetime
import java.io.ByteArrayOutputStream

sealed trait KafkaMonitoringApi[F[_], K, V] {
  def watch(implicit F: Console[F]): F[Unit]
  def watchFromEarliest(implicit F: Console[F]): F[Unit]
  def watchFrom(njt: NJTimestamp)(implicit C: Console[F]): F[Unit]
  def watchFrom(njt: String)(implicit C: Console[F]): F[Unit]

  def summaries(implicit C: Console[F]): F[Unit]

  def carbonCopyTo(other: KafkaTopic[F, K, V]): F[Unit]
}

object KafkaMonitoringApi {

  def apply[F[_]: Async, K, V](topic: KafkaTopic[F, K, V]): KafkaMonitoringApi[F, K, V] =
    new KafkaTopicMonitoring[F, K, V](topic)

  final private class KafkaTopicMonitoring[F[_]: Async, K, V](topic: KafkaTopic[F, K, V])
      extends KafkaMonitoringApi[F, K, V] with zoneddatetime {

    private def fetchData(aor: AutoOffsetReset): Stream[F, NJConsumerRecordWithError[K, V]] =
      topic.fs2Channel
        .updateConsumer(_.withAutoOffsetReset(aor).withEnableAutoCommit(false))
        .stream
        .map(m => topic.decode(m))

    private def printJackson(cr: NJConsumerRecordWithError[K, V])(implicit C: Console[F]): F[Unit] =
      Resource.fromAutoCloseable[F, ByteArrayOutputStream](Async[F].pure(new ByteArrayOutputStream())).use { bos =>
        for {
          _ <- C.println(cr.metaInfo(topic.context.settings.zoneId).timestamp.show)
          _ <- cr.key.leftTraverse(C.println)
          _ <- cr.value.leftTraverse(C.println)
          _ <- C.println {
            val aos: AvroOutputStream[NJConsumerRecord[K, V]] = AvroOutputStream
              .json[NJConsumerRecord[K, V]](
                NJConsumerRecord.avroCodec(topic.codec.keySerde.avroCodec, topic.codec.valSerde.avroCodec).avroEncoder)
              .to(bos)
              .build()
            aos.write(cr.toNJConsumerRecord)
            aos.close()
            bos.flush()
            bos.toString()
          }
          _ <- C.println("----------------------------------------")
        } yield ()
      }

    override def watchFrom(njt: NJTimestamp)(implicit C: Console[F]): F[Unit] = {
      val run: Stream[F, Unit] = for {
        kcs <- Stream.resource(topic.shortLiveConsumer)
        gtp <- Stream.eval(for {
          os <- kcs.offsetsForTimes(njt)
          e <- kcs.endOffsets
        } yield os.combineWith(e)(_.orElse(_)))
        _ <- topic.fs2Channel
          .updateConsumer(_.withEnableAutoCommit(false))
          .assign(gtp.mapValues(_.getOrElse(KafkaOffset(0))))
          .map(m => topic.decode(m))
          .evalMap(printJackson)
      } yield ()
      run.compile.drain
    }

    override def watchFrom(njt: String)(implicit C: Console[F]): F[Unit] =
      watchFrom(NJTimestamp(njt, topic.context.settings.zoneId))

    override def watch(implicit F: Console[F]): F[Unit] =
      fetchData(AutoOffsetReset.Latest).evalMap(printJackson).compile.drain

    override def watchFromEarliest(implicit F: Console[F]): F[Unit] =
      fetchData(AutoOffsetReset.Earliest).evalMap(printJackson).compile.drain

    override def summaries(implicit C: Console[F]): F[Unit] =
      topic.shortLiveConsumer.use { consumer =>
        for {
          num <- consumer.numOfRecords
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
