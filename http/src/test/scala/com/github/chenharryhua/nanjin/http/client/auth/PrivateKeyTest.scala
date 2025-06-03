package com.github.chenharryhua.nanjin.http.client.auth

import better.files.Resource
import cats.effect.IO
import cats.effect.std.Supervisor
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Paths
import scala.concurrent.duration.*

class PrivateKeyTest extends AnyFunSuite {

  /*
     pkcs8.key is generated from key.pem by
     openssl pkcs8 -topk8 -inform PEM -outform DER -nocrypt -in key.pem -out pkcs8.key
   */
  test("equality") {
    val pkcs8 = Paths.get(Resource.getUrl("private-pkcs8.key").getPath)
    val pem = Paths.get(Resource.getUrl("private-key.pem").getPath)
    assert(privateKeys.pem(pem).get.equals(privateKeys.pkcs8(pkcs8).get))
  }

  test("supervisor") {
    import cats.effect.Resource
    val problemOperation =
      (IO.println("problem computing") >> IO.raiseError[Int](new Exception("oops")).delayBy(5.second))
        .guarantee(IO.println("finish problem computing"))

    val run = for {
      sv <- Supervisor[IO]
      _ <- Resource.eval(IO.println("start"))
      _ <- Resource.eval(sv.supervise(problemOperation.foreverM))
      _ <- Resource.eval(IO.println("main compute"))
    } yield ()

    run.use(_ => IO.sleep(1.seconds)).unsafeRunSync()
    // problemOperation.unsafeRunSync()
    // assert(2 == run.guarantee(IO.println("accomplished")).unsafeRunSync)
  }
}
