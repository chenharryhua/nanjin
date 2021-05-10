package mtest

import cats.effect.IO

import scala.concurrent.ExecutionContext.Implicits.global
import cats.effect.Temporal

package object pipes {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Temporal[IO]     = IO.timer(global)
  val blocker: Blocker              = Blocker.liftExecutionContext(global)
}
