package mtest.database

import cats.effect.IO
import cats.implicits.catsSyntaxApplicativeId
import com.github.chenharryhua.nanjin.common.Secret
import com.github.chenharryhua.nanjin.database.*
import org.typelevel.doobie.ConnectionIO
import fs2.Stream
import munit.CatsEffectSuite

class DoobieMetaTest extends CatsEffectSuite {

  val postgres: Postgres =
    Postgres("unknown", Secret("unknown"), "localhost", 5432, "postgres")

  test("1.setter") {
    val username = "postgres"
    val password = "postgres"
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

    for {
      res <- stream.compile.lastOrError
      _ = assert(res == 42)
      connected <- nj.testConnection[IO]
    } yield assert(connected)
  }

}
