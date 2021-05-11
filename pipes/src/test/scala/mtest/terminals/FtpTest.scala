package mtest.terminals

import akka.stream.Materializer
import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.terminals.{AkkaFtpDownloader, AkkaFtpUploader}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
import cats.effect.unsafe.implicits.global

class FtpTest extends AnyFunSuite {

  val uploader   = new AkkaFtpUploader[IO](ftpSettins)
  val downloader = new AkkaFtpDownloader[IO](ftpSettins)

  val pathStr    = "ftp-test.txt"
  val testString = s"send a string to ftp and read it back ${Random.nextInt()}"

  val ts: Stream[IO, Byte] = Stream(testString).through(fs2.text.utf8Encode)

  implicit val mat = Materializer(akkaSystem)

  test("ftp should overwrite target file") {
    val action = ts.through(uploader.upload(pathStr)).compile.drain >>
      downloader.download(pathStr).through(fs2.text.utf8Decode).compile.string
    val readback = action.unsafeRunSync()
    assert(readback == testString)
  }
}
