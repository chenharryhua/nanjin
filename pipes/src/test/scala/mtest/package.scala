import cats.effect.{Blocker, ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)
  val blocker: Blocker              = Blocker.liftExecutionContext(global)
}
