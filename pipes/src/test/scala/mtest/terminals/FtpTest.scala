package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{FtpDownloader, FtpUploader}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
class FtpTest extends AnyFunSuite {

  val uploader   = FtpUploader[IO](ftpSettins)
  val downloader = FtpDownloader[IO](ftpSettins)

  val pathStr    = "ftp-test.txt"
  val testString = s"send a string to ftp and read it back ${Random.nextInt()}"

  val ts: Stream[IO, Byte] = Stream(testString).through(fs2.text.utf8.encode)

  test("ftp should overwrite target file") {
    val action = ts.through(uploader.upload(pathStr)).compile.drain >>
      downloader.download(pathStr, 100).through(fs2.text.utf8.decode).compile.string
    val readback = action.unsafeRunSync()
    assert(readback == testString)
  }
}
