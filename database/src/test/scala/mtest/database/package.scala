package mtest

import cats.effect.{ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.database.{DatabaseName, Host, Password, Port, Postgres, Username}

import scala.concurrent.ExecutionContext.Implicits.global

package object database {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]               = IO.timer(global)

  val postgres: Postgres =
    Postgres(Username("postgres"), Password("postgres"), Host("localhost"), Port(5432), DatabaseName("postgres"))

}
