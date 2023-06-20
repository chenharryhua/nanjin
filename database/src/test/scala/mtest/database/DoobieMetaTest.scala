package mtest.database

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxApplicativeId
import com.github.chenharryhua.nanjin.common.database.*
import com.github.chenharryhua.nanjin.database.NJHikari
import doobie.ConnectionIO
import doobie.hikari.HikariTransactor
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class DoobieMetaTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  val postgres: Postgres =
    Postgres(
      Username("unknown"),
      Password("unknown"),
      Host("localhost"),
      Port(5432),
      DatabaseName("postgres"))

  test("setter") {
    val username = Username("postgres")
    val password = Password("postgres")
    val nj = NJHikari(postgres)
      .set(_.setUsername("superceded by last update"))
      .set(_.setUsername(username.value))
      .set(_.setPassword(password.value))
    assert(nj.hikariConfig.getUsername == username.value)
    assert(nj.hikariConfig.getPassword == password.value)
    assert(nj.hikariConfig.getMaximumPoolSize == 10)

    val stream = for {
      tnx <- Stream.resource(HikariTransactor.fromHikariConfig[IO](nj.hikariConfig))
      n <- Stream.eval(tnx.trans.apply(42.pure[ConnectionIO]))
    } yield n

    val res = stream.compile.lastOrError.unsafeRunSync()
    assert(res == 42)
  }
}
