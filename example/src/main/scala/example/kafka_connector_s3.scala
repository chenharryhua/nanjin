package example

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits.{catsSyntaxApplicativeByName, catsSyntaxSemigroup, toTraverseOps}
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
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

  private def logMetrics(mtx: Metrics[IO]): Resource[IO, Kleisli[IO, CCR, Unit]] =
    for {
      countRate <- mtx.meter("count.rate", _.withUnit(_.COUNT).enable(true))
      byteRate <- mtx.meter("bytes.rate", _.withUnit(_.BYTES).enable(true))
      keySize <- mtx.histogram("key.size", _.withUnit(_.BYTES).enable(true))
      valSize <- mtx.histogram("val.size", _.withUnit(_.BYTES).enable(true))
      goodNum <- mtx.counter("good.records", _.enable(true))
      badNum <- mtx.counter("bad.records", _.asRisk.enable(true))
      idle <- mtx.idleGauge("idle", _.enable(true))
    } yield Kleisli { (ccr: CCR) =>
      val ks: Option[Long] = ccr.record.serializedKeySize.map(_.toLong)
      val vs: Option[Long] = ccr.record.serializedValueSize.map(_.toLong)

      idle.run(()) *>
        ks.traverse(keySize.run) *> vs.traverse(valSize.run) *>
        (ks |+| vs).traverse(byteRate.run) *> countRate.run(1) *>
        goodNum.run(1).whenA(ccr.record.value.isSuccess) *>
        badNum.run(1).whenA(ccr.record.value.isFailure)
    }

  private val root: Url              = Url.parse("s3a://bucket_name") / "folder_name"
  private val hadoop: HadoopText[IO] = NJHadoop[IO](new Configuration).text

  aws_task_template.task
    .service("dump kafka topic to s3")
    .eventStream { ga =>
      val jackson = JacksonFile(_.Uncompressed)
      val sink: Pipe[IO, Chunk[String], Int] = // rotate files every 5 minutes
        hadoop.sink(Policy.crontab(_.every5Minutes), ga.zoneId)(tick => root / jackson.ymdFileName(tick))
      ga.facilitate("abc")(logMetrics).use { decode =>
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
