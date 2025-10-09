package mtest.database

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxApplicativeId
import com.github.chenharryhua.nanjin.common.database.*
import com.github.chenharryhua.nanjin.database.DBConfig
import doobie.ConnectionIO
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import eu.timepit.refined.auto.*

class DoobieMetaTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  val postgres: Postgres =
    Postgres(Username("unknown"), Password("unknown"), Host("localhost"), 5432, DatabaseName("postgres"))

  test("setter") {
    val username: Username = Username("postgres")
    val password: Password = Password("postgres")
    val nj = DBConfig(postgres)
      .set(_.setUsername("superceded by last update"))
      .set(_.setUsername(username.value))
      .set(_.setPassword(password.value))
    assert(nj.hikariConfig.getUsername == username.value)
    assert(nj.hikariConfig.getPassword == password.value)
    assert(nj.hikariConfig.getMaximumPoolSize == 10)

    val stream: Stream[IO, Int] = for {
      tnx <- nj.transactorS[IO](None)
      n <- Stream.eval(tnx.trans.apply(42.pure[ConnectionIO]))
    } yield n

    val res: Int = stream.compile.lastOrError.unsafeRunSync()
    assert(res === 42)

    assert(nj.testConnection[IO].unsafeRunSync())
  }

}
