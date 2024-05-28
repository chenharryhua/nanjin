package example

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits.catsSyntaxSemigroup
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.messages.kafka.CRMetaInfo
import com.github.chenharryhua.nanjin.messages.kafka.codec.gr2Jackson
import com.github.chenharryhua.nanjin.terminals.{HadoopText, JacksonFile, NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Pipe
import fs2.kafka.{commitBatchWithin, AutoOffsetReset, CommittableConsumerRecord}
import io.circe.syntax.EncoderOps
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration.DurationInt
import scala.util.Try

object kafka_connector_s3 {
  val ctx: KafkaContext[IO] = KafkaContext[IO](KafkaSettings.local)

  private type CCR = CommittableConsumerRecord[IO, Unit, Try[GenericData.Record]]

  private def decoder(agent: Agent[IO]): Resource[IO, Kleisli[IO, CCR, String]] = {
    val name: String = "Kafka record"
    def calcSize(ccr: CCR): Long =
      (ccr.record.serializedKeySize |+| ccr.record.serializedValueSize).getOrElse(0).toLong

    for {
      action <- agent
        .action(name)
        .retry((ccr: CCR) => IO.fromTry(ccr.record.value.flatMap(gr2Jackson(_))))
        .buildWith(_.tapError(CRMetaInfo(_).zoned(agent.zoneId).asJson))
      flow <- agent.flowMeter(name, _.withUnit(_.BYTES))
    } yield Kleisli { (ccr: CCR) =>
      action.run(ccr).flatTap(_ => flow.update(calcSize(ccr)))
    }
  }

  private val root: NJPath           = NJPath("s3a://bucket_name") / "folder_name"
  private val hadoop: HadoopText[IO] = NJHadoop[IO](new Configuration).text

  aws_task_template.task
    .service("dump kafka topic to s3")
    .eventStream { ga =>
      val jackson = JacksonFile(_.Uncompressed)
      val sink: Pipe[IO, String, Nothing] = // rotate files every 5 minutes
        hadoop.sink(policies.crontab(_.every5Minutes), ga.zoneId)(tick => root / jackson.ymdFileName(tick))
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
          .through(sink)
          .compile
          .drain
      }
    }
    .evalTap(console.text[IO])
}
