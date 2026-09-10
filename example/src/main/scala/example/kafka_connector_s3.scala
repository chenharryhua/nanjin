package example

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all.{catsSyntaxApplicativeByName, catsSyntaxSemigroup, toTraverseOps}
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.metrics.MetricsHub
import com.github.chenharryhua.nanjin.kafka.KafkaContext
import com.github.chenharryhua.nanjin.kafka.config.KafkaSettings
import com.github.chenharryhua.nanjin.kafka.connector.PullError
import com.github.chenharryhua.nanjin.terminals.{Hadoop, JacksonFile, RotateFile}
import fs2.Pipe
import fs2.kafka.{commitBatchWithin, AutoOffsetReset, CommittableConsumerRecord}
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.urlToUrlDsl
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import squants.information.Bytes

import scala.concurrent.duration.DurationInt

/** Example: stream an Avro Kafka topic to S3 as rotated Jackson files, with guard metrics, and a companion
  * job that prunes obsolete date folders.
  *
  *   - `dump` consumes a topic as generic records, publishes throughput/error metrics through a guard
  *     service, commits offsets in batches, and writes the values into time-partitioned files on S3 that
  *     rotate on a schedule.
  *   - `retention` runs on a daily cron and deletes date folders older than the retention window.
  *
  * The Kafka/S3 coordinates (`s3a://bucket_name/...`, topic and group ids) are placeholders — substitute your
  * own. Both values are `fs2.Stream`s of guard `Event`s; run one by compiling and draining it.
  */
object kafka_connector_s3 {
  val ctx: KafkaContext[IO] = KafkaContext[IO](KafkaSettings.local)

  /** A committable record whose value is either a decoded Avro record or a `PullError` (a record that failed
    * to deserialize). The `Either` lets the pipeline count bad records without aborting the stream.
    */
  private type CCR =
    CommittableConsumerRecord[IO, Unit, Either[PullError, GenericData.Record]]

  /** Register the per-record metrics and return a `Kleisli` that updates them for each consumed record.
    *
    * The gauges/counters/meters/histograms are acquired as a `Resource` so they are registered on start and
    * removed on release. `bad.records` is flagged `asRisk` so decode failures surface as a risk signal rather
    * than ordinary volume.
    */
  private def logMetrics(mtx: MetricsHub[IO]): Resource[IO, Kleisli[IO, CCR, Unit]] =
    for {
      idle <- mtx.idleGauge("idle", _.enable(true))
      goodNum <- mtx.counter("good.records", _.enable(true))
      badNum <- mtx.counter("bad.records", _.asRisk.enable(true))
      countRate <- mtx.meter("count.rate", _.enable(true))
      byteRate <- mtx.meter("bytes.rate", _.enable(true).withUnit(Bytes))
      keySize <- mtx.histogram("key.size", _.enable(true).withUnit(Bytes))
      valSize <- mtx.histogram("val.size", _.enable(true).withUnit(Bytes))
    } yield Kleisli { (ccr: CCR) =>
      // per record: mark liveness, record key/value sizes and throughput, and tally good vs bad decodes
      val ks: Option[Long] = ccr.record.serializedKeySize.map(_.toLong)
      val vs: Option[Long] = ccr.record.serializedValueSize.map(_.toLong)

      idle.wakeUp *>
        ks.traverse(keySize.update) *> vs.traverse(valSize.update) *>
        (ks |+| vs).traverse(byteRate.mark) *> countRate.mark(1) *>
        goodNum.inc(1).whenA(ccr.record.value.isRight) *>
        badNum.inc(1).whenA(ccr.record.value.isLeft)
    }

  private val root: Url = Url.parse("s3a://bucket_name") / "folder_name"
  private val hadoop = Hadoop[IO](new Configuration)

  /** Consume the topic and dump its records to S3 as rotated, uncompressed Jackson files.
    *
    * Offsets are committed with auto-commit off and `commitBatchWithin`, so commits are explicit and batched.
    * Records are written into `root/<ymd file name>` and the sink rotates to a new file every 5 minutes.
    *
    * Note the decode handling below: it keeps decoded values and raises an exception containing `PullError`
    * metadata on a failed decode. This is fine here because `bad.records` is counted in `logMetrics` before
    * this point; adjust if you would rather route bad records elsewhere than fail the stream.
    */
  val dump: fs2.Stream[IO, Event] =
    aws_task_template.task.service("dump kafka topic to s3").eventStream { ga =>
      val jackson = JacksonFile(_.Uncompressed)
      val sink: Pipe[IO, GenericRecord, RotateFile] = // rotate files every 5 minutes
        hadoop.rotateSink(ga.zoneId, _.crontab(_.every5Minutes))(root / jackson.ymdFileName(_)).jackson
      ga.facilitate("abc")(logMetrics).use { _ =>
        ctx
          .consumeGenericRecord("any.kafka.avro.topic")
          .updateConfig(
            _.withGroupId("group.id")
              .withAutoOffsetReset(AutoOffsetReset.Latest)
              .withEnableAutoCommit(false)
              .withMaxPollRecords(2000))
          .subscribe
          // commit offsets in batches (up to 1000 records or every 5s) as a side stream via observe
          .observe(_.map(_.offset).through(commitBatchWithin[IO](1000, 5.seconds)).drain)
          .map(_.record.value.toOption.get) // keep decoded values; rethrow on a PullError
          .through(sink)
          .compile
          .drain
      }
    }

  /** Retention job: on a daily 1:00am cron, delete date folders under `root` older than 8 days. The `root`
    * here is a placeholder distinct from `dump`'s; point it at the folder tree you want pruned.
    */
  val retention: fs2.Stream[IO, Event] =
    aws_task_template.task.service("delete.obsolete.folder").eventStreamS { agent =>
      val root: Url = Url.parse("s3://abc-efg-hij/klm")
      agent.tickScheduled(_.crontab(_.daily.oneAM)).evalMap { tick =>
        hadoop.dateFolderRetention(root, tick.local(_.conclude).toLocalDate, 8)
      }
    }
}
