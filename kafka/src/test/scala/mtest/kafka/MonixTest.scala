package mtest.kafka

import cats.derived.auto.show._
import cats.effect.ConcurrentEffect
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{KafkaSettings, MonixKafkaContext, TopicDef}
import io.circe.generic.auto._
import monix.eval.Task
import monix.eval.instances.CatsConcurrentEffectForTask
import monix.execution.Scheduler
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

class MonixTest extends AnyFunSuite {
  implicit val scheduler: Scheduler = Scheduler.global
  val options: Task.Options         = Task.defaultOptions.withSchedulerFeatures(scheduler)

  implicit lazy val catsEffect: ConcurrentEffect[Task] =
    new CatsConcurrentEffectForTask()(scheduler, options)
  val ctx: MonixKafkaContext = KafkaSettings.local.monixContext

  test("monix should just work") {
    val topic = TopicDef[String, trip_record]("nyc_yellow_taxi_trip_data").in(ctx)
    val task =
      topic.fs2Channel.consume
        .map(m => topic.decoder(m).logRecord.run)
        .take(3)
        .map(println)
        .compile
        .drain
    task.runSyncUnsafe(10.seconds)
  }
}
