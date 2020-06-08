import java.net.InetAddress

import akka.actor.ActorSystem
import akka.stream.alpakka.ftp.FtpCredentials.NonAnonFtpCredentials
import akka.stream.alpakka.ftp.{FtpCredentials, FtpSettings}
import cats.effect.{Blocker, ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {

  implicit val cs: ContextShift[IO]    = IO.contextShift(global)
  implicit val timer: Timer[IO]        = IO.timer(global)
  implicit val akkaSystem: ActorSystem = ActorSystem("nj-devices")

  val blocker: Blocker = Blocker.liftExecutionContext(global)

  val cred       = FtpCredentials.create("chenh", "test")
  val ftpSettins = FtpSettings(InetAddress.getLocalHost).withCredentials(cred)
}
