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
    val pem   = Paths.get(Resource.getUrl("private-key.pem").getPath)
    assert(privateKeys.pem(pem).get.equals(privateKeys.pkcs8(pkcs8).get))
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
