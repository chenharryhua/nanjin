package mtest.spark.ftp

import akka.stream.Materializer
import akka.stream.alpakka.ftp.{FtpCredentials, FtpSettings, FtpsSettings, SftpSettings}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.ftp.{ftpSink, ftpSource}
import com.github.chenharryhua.nanjin.spark.injection.*
import io.circe.generic.auto.*
import kantan.csv.generic.*
import kantan.csv.java8.*
import mtest.spark.akkaSystem
import mtest.spark.persist.{Tablet, TabletData}
import org.apache.commons.net.PrintCommandListener
import org.apache.commons.net.ftp.FTPClient
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*
import java.io.PrintWriter
import java.net.InetAddress
import squants.information.Kilobytes
class FtpTest extends AnyFunSuite {
  val cred = FtpCredentials.create("chenh", "test")

  val ftpSettins =
    FtpSettings(InetAddress.getLocalHost)
      .withPort(21)
      .withCredentials(cred)
      .withPassiveMode(true)
      .withConfigureConnection { (ftpClient: FTPClient) =>
        ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true))
        ftpClient.setRemoteVerificationEnabled(false)
      }
  val sink   = ftpSink[IO](ftpSettins)
  val source = ftpSource[IO](ftpSettins)

  val roosterSteam = fs2.Stream.emits(TabletData.data)

  val ftps        = FtpsSettings(InetAddress.getLocalHost)
  val ftpsSink1   = ftpSink[IO](ftps)
  val ftpsSource1 = ftpSource[IO](ftps)

  val sftp        = SftpSettings(InetAddress.getLocalHost)
  val sftpSink2   = ftpSink[IO](sftp)
  val sftpSource2 = ftpSource[IO](sftp)

  implicit val mat: Materializer = Materializer(akkaSystem)

  test("json") {
    val path = "tablet.json"
    val rst  = roosterSteam.through(sink.json(path)) >> source.json[Tablet](path)
    assert(rst.compile.toList.unsafeRunSync() == TabletData.data)
  }

  test("jackson") {
    val path = "tablet.jackson.json"
    val rst = roosterSteam.through(sink.jackson(path, Tablet.avroCodec.avroEncoder)) >> source
      .jackson[Tablet](path, Tablet.avroCodec.avroDecoder)
    assert(rst.compile.toList.unsafeRunSync() == TabletData.data)
  }

  test("csv") {
    val path = "tablet.csv"
    val rst  = roosterSteam.through(sink.csv(path, Kilobytes(10))) >> source.csv[Tablet](path, 100)
    assert(rst.compile.toList.unsafeRunSync() == TabletData.data)
  }

  test("text") {
    val path = "tablet.text"
    val rst  = roosterSteam.map(_.toString).through(sink.text(path)) >> source.text(path)
    rst.take(3).compile.toList.unsafeRunSync().foreach(println)
  }

}
