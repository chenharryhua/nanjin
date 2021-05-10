package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.database.{DatabaseName, Host, Password, Port, Postgres, Username}

import scala.concurrent.ExecutionContext.Implicits.global
import cats.effect.Temporal

package object database {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Temporal[IO]               = IO.timer(global)

  val postgres: Postgres =
    Postgres(Username("postgres"), Password("postgres"), Host("localhost"), Port(5432), DatabaseName("postgres"))

}
