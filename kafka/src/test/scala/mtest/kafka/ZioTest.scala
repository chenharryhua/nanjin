package mtest.kafka

import cats.derived.auto.show._
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka._
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.interop.catz._
import zio.interop.catz.implicits.ioTimer
import zio.random.Random
import zio.system.System
import zio.{Runtime, Task}

class ZioTest extends AnyFunSuite {
  type Environment = Clock with Console with System with Random with Blocking

  implicit val runtime: Runtime[zio.ZEnv] = Runtime.default

  val ctx: KafkaContext[Task] = KafkaSettings.local.zioContext

  val topic: KafkaTopic[Task, Array[Byte], trip_record] =
    TopicDef[Array[Byte], trip_record](TopicName("nyc_yellow_taxi_trip_data")).in(ctx)

  test("zio should just work.") {
    val task = topic.fs2Channel.stream
      .map(m => topic.decoder(m).tryDecode)
      .map(_.toEither)
      .rethrow
      .take(1)
      .map(_.toString)
      .map(println)
      .compile
      .drain
      .run
    runtime.unsafeRun(task)
  }

  test("zio should work for akka.") {
    val task = topic
      .updateAkkaConsumerSettings(_.withClientId("akka-test"))
      .akkaChannel(akkaSystem)
      .source
      .map(x => topic.decoder(x).decodeValue)
      .take(1)
      .map(_.toString)
      .map(println)
      .runWith(akkaSinks.ignore)
    runtime.unsafeRun(task)
  }
}
