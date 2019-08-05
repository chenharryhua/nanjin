package com.github.chenharryhua.nanjin.kafka

import org.scalatest.FunSuite
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.interop.catz.implicits.ioTimer
import zio.interop.catz.{taskEffectInstances, zioContextShift}
import zio.random.Random
import zio.system.System
import zio.{DefaultRuntime, Exit, Runtime, ZIO}
import cats.implicits._

class ZioTest extends FunSuite with ShowKafkaMessage {
  type Environment = Clock with Console with System with Random with Blocking

  implicit val runtime: Runtime[Environment] = new DefaultRuntime {}

  val ctx: ZioKafkaContext = KafkaSettings.local.zioContext

  test("zio should behave like IO.") {
    val task = ctx
      .topic[String, Payment]("cc_payments")
      .fs2Channel
      .consumeMessages
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
}
