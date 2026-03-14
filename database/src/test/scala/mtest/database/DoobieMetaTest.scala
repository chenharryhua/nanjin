package mtest.database

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxApplicativeId
import com.github.chenharryhua.nanjin.database.*
import doobie.ConnectionIO
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import io.github.iltotore.iron.*

class DoobieMetaTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  val postgres: Postgres =
    Postgres(
      "unknown",
      "unknown",
      "localhost",
      5432,
      "postgres"
    )

  test("setter") {
    val username: Username = "postgres"
    val password: Password = "postgres"
    val nj = DBConfig(postgres)
      .set(_.setUsername("superceded by last update"))
      .set(_.setUsername(username))
      .set(_.setPassword(password))
    assert(nj.hikariConfig.getUsername == username)
    assert(nj.hikariConfig.getPassword == password)
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
