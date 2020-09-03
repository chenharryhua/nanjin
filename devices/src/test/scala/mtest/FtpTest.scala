package mtest

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.devices.{AkkaFtpDownloader, AkkaFtpUploader}
import fs2.Stream
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

class FtpTest extends AnyFunSuite {

  val uploader   = new AkkaFtpUploader[IO](ftpSettins)
  val downloader = new AkkaFtpDownloader[IO](ftpSettins)

  val pathStr    = "ftp-test.txt"
  val testString = "send a string to ftp and read it back"

  val ts: Stream[IO, Byte] =
    Stream(testString).through(fs2.text.utf8Encode)

 
  ignore("identity ftp") {
    val action = ts.through(uploader.upload(pathStr)).compile.toList >>
      downloader.download(pathStr).through(fs2.text.utf8Decode).compile.lastOrError

    assert(action.unsafeRunSync() == testString)

  }
}
