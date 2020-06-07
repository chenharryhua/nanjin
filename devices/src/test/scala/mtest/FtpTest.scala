package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.devices.AkkaFtpUploader
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class FtpTest extends AnyFunSuite {

  val ftp = new AkkaFtpUploader[IO](ftpSettins)

  val pathStr    = "test.txt"
  val testString = "save string to ftp"

  val ts: Stream[IO, Byte] =
    Stream(testString).through(fs2.text.utf8Encode)

  test("identity ftp") {
    println(ts.through(ftp.upload(pathStr)).compile.toList.unsafeRunSync())
  }
}
