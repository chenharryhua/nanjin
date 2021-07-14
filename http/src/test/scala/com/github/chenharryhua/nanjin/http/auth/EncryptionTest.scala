package com.github.chenharryhua.nanjin.http.auth

import better.files.Resource
import cats.effect.IO
import cats.effect.std.Supervisor
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.concurrent.duration.*

class EncryptionTest extends AnyFunSuite {
  test("pkcs8") {
    assert(encryption.pkcs8(new File(Resource.getUrl("pkcs8.key").getPath)).getAlgorithm == "RSA")
  }

  test("supervisor") {
    val run = for {
      _ <- IO(1)
      fib <- Supervisor[IO]
        .use(s => s.supervise(IO.sleep(1.second) >> IO.raiseError(new Exception)).guarantee(IO.println("raise error")))
        .guarantee(IO.println("supervisor done"))
      _ <- IO.println("compute")
      _ <- IO.sleep(3.seconds)
      r <- IO(2)
    } yield r
    assert(2 == run.guarantee(IO.println("accomplished")).unsafeRunSync)
  }
}
