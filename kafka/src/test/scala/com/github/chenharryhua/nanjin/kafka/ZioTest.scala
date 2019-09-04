package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import com.github.chenharryhua.nanjin.codec.ShowKafkaMessage
import org.scalatest.FunSuite
import org.scalatest.funsuite.AnyFunSuite
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.interop.catz.implicits.ioTimer
import zio.interop.catz.{taskEffectInstances, zioContextShift}

import zio.random.Random
import zio.system.System
import zio.{DefaultRuntime, Runtime}

class ZioTest extends AnyFunSuite with ShowKafkaMessage {
  type Environment = Clock with Console with System with Random with Blocking

  implicit val runtime: Runtime[Environment] = new DefaultRuntime {}

  val ctx: ZioKafkaContext = KafkaSettings.local.zioContext

  ignore("zio should just work.") {
    val chn = ctx.topic[String, Payment]("cc_payments").fs2Channel
    val task = chn.consume
      .map(chn.messageDecoder.tryDecode)
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
    val task = ctx.topic[String, Payment]("cc_payments").akkaResource.use { chn =>
      chn
        .updateConsumerSettings(_.withClientId("akka-test"))
        .consume
        .map(x => chn.messageDecoder.decodeValue(x))
        .take(3)
        .map(_.show)
        .map(println)
        .runWith(chn.ignoreSink)(chn.materializer)
    }
    runtime.unsafeRun(task)
  }
}
