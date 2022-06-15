package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{FtpDownloader, FtpUploader}
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.generic.auto.*
import kantan.csv.{RowDecoder, RowEncoder}
import kantan.csv.generic.*
import org.scalatest.funsuite.AnyFunSuite
import squants.information.Kilobytes

import scala.util.Random

final case class Tablet(a: Int, b: Long)

object Tablet {
  val data: List[Tablet]              = List(Tablet(1, 2), Tablet(4, 5), Tablet(7, 8))
  val avroEncoder                     = AvroEncoder[Tablet]
  val avroDecoder                     = AvroDecoder[Tablet]
  implicit val re: RowEncoder[Tablet] = shapeless.cachedImplicit
  implicit val rd: RowDecoder[Tablet] = shapeless.cachedImplicit
}

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

  val sink   = FtpUploader[IO](ftpSettins)
  val source = FtpDownloader[IO](ftpSettins)

  val tabletSteam = fs2.Stream.emits(Tablet.data)

  test("json") {
    val path = "tablet.json"
    val rst  = tabletSteam.through(sink.circe(path)) >> source.circe[Tablet](path, 10)
    assert(rst.compile.toList.unsafeRunSync() == Tablet.data)
  }

  test("jackson") {
    val path = "tablet.jackson.json"
    val rst = tabletSteam.through(sink.jackson(path, Tablet.avroEncoder)) >> source
      .jackson[Tablet](path, 10, Tablet.avroDecoder)
    assert(rst.compile.toList.unsafeRunSync() == Tablet.data)
  }

  test("csv") {
    val path = "tablet.csv"
    val rst  = tabletSteam.through(sink.kantan(path, Kilobytes(10))) >> source.kantan[Tablet](path, 100)
    assert(rst.compile.toList.unsafeRunSync() == Tablet.data)
  }

  test("text") {
    val path = "tablet.text"
    val rst  = tabletSteam.map(_.toString).through(sink.text(path)) >> source.text(path, 10)
    rst.take(3).compile.toList.unsafeRunSync().foreach(println)
  }

}
