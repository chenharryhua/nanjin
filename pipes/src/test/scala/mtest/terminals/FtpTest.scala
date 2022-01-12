package mtest.terminals

import akka.stream.Materializer
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{FtpDownloader, FtpUploader}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
import eu.timepit.refined.auto.*
class FtpTest extends AnyFunSuite {

  val uploader   = FtpUploader[IO](ftpSettins)
  val downloader = FtpDownloader[IO](ftpSettins)

  val pathStr    = "ftp-test.txt"
  val testString = s"send a string to ftp and read it back ${Random.nextInt()}"

  val ts: Stream[IO, Byte] = Stream(testString).through(fs2.text.utf8.encode)

  implicit val mat = Materializer(akkaSystem)

  test("ftp should overwrite target file") {
    val action = ts.through(uploader.withChunkSize(100).upload(pathStr)).compile.drain >>
      downloader.withChunkSize(100).download(pathStr).through(fs2.text.utf8.decode).compile.string
    val readback = action.unsafeRunSync()
    assert(readback == testString)
  }
}
