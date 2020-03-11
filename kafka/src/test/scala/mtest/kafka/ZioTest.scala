package mtest.kafka

import cats.derived.auto.show._
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka._
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite
import zio.Runtime
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.interop.catz._
import zio.interop.catz.implicits.ioTimer
import zio.random.Random
import zio.system.System

@Ignore
class ZioTest extends AnyFunSuite {
  type Environment = Clock with Console with System with Random with Blocking

  implicit val runtime = Runtime.default

  val ctx: ZioKafkaContext = KafkaSettings.local.zioContext

  val topic = TopicDef[Array[Byte], trip_record](TopicName("nyc_yellow_taxi_trip_data")).in(ctx)

  test("zio should just work.") {
    val task = topic.fs2Channel.stream
      .map(m => topic.decoder(m).tryDecode)
      .map(_.toEither)
      .rethrow
      .take(1)
      .map(_.show)
      .map(println)
      .compile
      .drain
      .run
    runtime.unsafeRun(task)
  }

  test("zio should work for akka.") {
    val task = topic.akkaChannel
      .withConsumerSettings(_.withClientId("akka-test"))
      .source
      .map(x => topic.decoder(x).decodeValue)
      .take(1)
      .map(_.show)
      .map(println)
      .runWith(akkaSinks.ignore)
    runtime.unsafeRun(task)
  }
}
