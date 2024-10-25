package example

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.metrics.NJMetrics
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.messages.kafka.CRMetaInfo
import com.github.chenharryhua.nanjin.messages.kafka.codec.gr2Jackson
import com.github.chenharryhua.nanjin.terminals.{HadoopText, JacksonFile, NJHadoop}
import eu.timepit.refined.auto.*
import fs2.kafka.{commitBatchWithin, AutoOffsetReset, CommittableConsumerRecord}
import fs2.{Chunk, Pipe}
import io.circe.syntax.EncoderOps
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
      meter <- mtx.meter("rate", _.withUnit(_.COUNT).enable(true))
      keySize <- mtx.histogram("key.size", _.withUnit(_.BYTES).enable(true))
      valSize <- mtx.histogram("val.size", _.withUnit(_.BITS).enable(true))
    } yield Kleisli { (ccr: CCR) =>
      ccr.record.serializedKeySize.map(_.toLong).traverse(keySize.update) *>
        ccr.record.serializedValueSize.map(_.toLong).traverse(valSize.update) *>
        meter.update(1)
    }

  private def decoder(agent: Agent[IO]): Resource[IO, Kleisli[IO, CCR, String]] = {
    val name: String = "kafka.record"
    for {
      action <- agent
        .action(name)
        .retry((ccr: CCR) => IO.fromTry(ccr.record.value.flatMap(gr2Jackson(_))))
        .buildWith(_.tapError((cr, _) => CRMetaInfo(cr).zoned(agent.zoneId).asJson))
      update <- logMetrics(agent.metrics("name"))
    } yield Kleisli { (ccr: CCR) =>
      update(ccr) >> action.run(ccr)
    }
  }

  private val root: Url              = Url.parse("s3a://bucket_name") / "folder_name"
  private val hadoop: HadoopText[IO] = NJHadoop[IO](new Configuration).text

  aws_task_template.task
    .service("dump kafka topic to s3")
    .eventStream { ga =>
      val jackson = JacksonFile(_.Uncompressed)
      val sink: Pipe[IO, Chunk[String], Int] = // rotate files every 5 minutes
        hadoop.sink(Policy.crontab(_.every5Minutes), ga.zoneId)(tick => root / jackson.ymdFileName(tick))
      decoder(ga).use { decode =>
        ctx
          .consume("any.kafka.topic")
          .updateConfig(
            _.withGroupId("group.id")
              .withAutoOffsetReset(AutoOffsetReset.Latest)
              .withEnableAutoCommit(false)
              .withMaxPollRecords(2000))
          .genericRecords
          .observe(_.map(_.offset).through(commitBatchWithin[IO](1000, 5.seconds)).drain)
          .evalMap(decode.run)
          .chunks
          .through(sink)
          .compile
          .drain
      }
    }
    .evalTap(console.text[IO])
}
