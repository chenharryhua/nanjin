package mtest.terminals

import akka.stream.Materializer
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.terminals.{AkkaFtpDownloader, AkkaFtpUploader}
import fs2.Stream
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class FtpTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  val uploader   = new AkkaFtpUploader[IO](ftpSettins)
  val downloader = new AkkaFtpDownloader[IO](ftpSettins)

  val pathStr    = "ftp-test.txt"
  val testString = s"send a string to ftp and read it back ${Random.nextInt()}"

  val ts: Stream[IO, Byte] = Stream(testString).through(fs2.text.utf8Encode)

  implicit val mat: Materializer = Materializer(akkaSystem)

  "FTP" - {
    "ftp should overwrite target file" in {
      val action = ts.through(uploader.upload(pathStr)).compile.drain >>
        downloader.download(pathStr).through(fs2.text.utf8Decode).compile.string

      action.asserting(_ shouldBe testString)
    }
  }
}
