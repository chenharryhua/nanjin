package mtest

import cats.implicits._
import com.github.chenharryhua.nanjin.kafka._
import cats.derived.auto.show._ 
import org.scalatest.funsuite.AnyFunSuite
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.interop.catz._
import zio.interop.catz.implicits.ioTimer
import zio.random.Random
import zio.system.System
import zio.{DefaultRuntime, Runtime}
import com.github.chenharryhua.nanjin.codec._
import com.github.chenharryhua.nanjin.codec.show._ 
import com.github.chenharryhua.nanjin.codec.bitraverse._

class ZioTest extends AnyFunSuite {
  type Environment = Clock with Console with System with Random with Blocking

  implicit val runtime: Runtime[Environment] = new DefaultRuntime {}

  val ctx: ZioKafkaContext = KafkaSettings.local.zioContext

  ignore("zio should just work.") {
    val topic = ctx.topic[String, Payment]("cc_payments")
    val task = topic.fs2Channel.consume
      .map(m => topic.decoder(m).tryDecode)
      .map(_.toEither)
      .rethrow
      .take(3)
      .map(_.show)
      .map(println)
      .compile
      .drain
      .run
    runtime.unsafeRun(task)
  }

  ignore("zio should work for akka.") {
    val topic = ctx.topic[String, Payment]("cc_payments")
    val task = topic.akkaResource.use { chn =>
      chn
        .updateConsumerSettings(_.withClientId("akka-test"))
        .consume
        .map(x => topic.decoder(x).decodeValue)
        .take(3)
        .map(_.show)
        .map(println)
        .runWith(chn.ignoreSink)(ctx.materializer.value)
    }
    runtime.unsafeRun(task)
  }
}
