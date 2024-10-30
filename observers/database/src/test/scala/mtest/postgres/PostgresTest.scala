package mtest.postgres

import org.scalatest.funsuite.AnyFunSuite

class PostgresTest extends AnyFunSuite {

  test("postgres") {

//    val session: Resource[IO, Session[IO]] =
//      Session.single[IO](
//        host = "localhost",
//        port = 5432,
//        user = "postgres",
//        database = "postgres",
//        password = Some("postgres"),
//        debug = true)
//
//    val cmd: Command[skunk.Void] =
//      sql"""CREATE TABLE IF NOT EXISTS log (
//              info json NULL,
//              id SERIAL,
//              timestamp timestamptz default current_timestamp)""".command

    // val run = session.use(_.execute(cmd)) >>
    //  service.evalTap(console.text[IO]).through(PostgresObserver(session).observe("log")).compile.drain

    // run.unsafeRunSync()
  }

}
