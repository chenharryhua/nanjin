package example

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.metrics.NJMetrics
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.messages.kafka.codec.gr2Jackson
import com.github.chenharryhua.nanjin.terminals.{HadoopText, JacksonFile, NJHadoop}
import eu.timepit.refined.auto.*
import fs2.kafka.{commitBatchWithin, AutoOffsetReset, CommittableConsumerRecord}
import fs2.{Chunk, Pipe}
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.urlToUrlDsl
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration.DurationInt
import scala.util.Try

object kafka_connector_s3 {
  val ctx: KafkaContext[IO] = KafkaContext[IO](KafkaSettings.local)

  private type CCR = CommittableConsumerRecord[IO, Unit, Try[GenericData.Record]]

  private def logMetrics(mtx: NJMetrics[IO]): Resource[IO, Kleisli[IO, CCR, Unit]] =
    for {
      countRate <- mtx.meter("count.rate", _.withUnit(_.COUNT).enable(true))
      byteRate <- mtx.meter("byte.rate", _.withUnit(_.BYTES))
      keySize <- mtx.histogram("key.size", _.withUnit(_.BYTES).enable(true))
      valSize <- mtx.histogram("val.size", _.withUnit(_.BITS).enable(true))
    } yield Kleisli { (ccr: CCR) =>
      val ks: Long = ccr.record.serializedKeySize.map(_.toLong).getOrElse(0L)
      val vs: Long = ccr.record.serializedValueSize.map(_.toLong).getOrElse(0L)
      keySize.update(ks) *> valSize.update(vs) *> byteRate.update(ks + vs) *> countRate.update(1)
    }

  private val root: Url              = Url.parse("s3a://bucket_name") / "folder_name"
  private val hadoop: HadoopText[IO] = NJHadoop[IO](new Configuration).text

  aws_task_template.task
    .service("dump kafka topic to s3")
    .eventStream { ga =>
      val jackson = JacksonFile(_.Uncompressed)
      val sink: Pipe[IO, Chunk[String], Int] = // rotate files every 5 minutes
        hadoop.sink(Policy.crontab(_.every5Minutes), ga.zoneId)(tick => root / jackson.ymdFileName(tick))
      ga.metrics("abc")(logMetrics).use { decode =>
        ctx
          .consume("any.kafka.topic")
          .updateConfig(
            _.withGroupId("group.id")
              .withAutoOffsetReset(AutoOffsetReset.Latest)
              .withEnableAutoCommit(false)
              .withMaxPollRecords(2000))
          .genericRecords
          .observe(_.map(_.offset).through(commitBatchWithin[IO](1000, 5.seconds)).drain)
          .evalMap(x => decode.run(x) >> IO.fromTry(x.record.value.flatMap(gr2Jackson(_))))
          .chunks
          .through(sink)
          .compile
          .drain
      }
    }
    .evalTap(console.text[IO])
}
