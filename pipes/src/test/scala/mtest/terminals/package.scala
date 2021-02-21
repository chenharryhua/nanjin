package mtest

import akka.actor.ActorSystem
import akka.stream.alpakka.ftp.{FtpCredentials, FtpSettings}
import org.apache.commons.net.PrintCommandListener
import org.apache.commons.net.ftp.FTPClient

import java.io.PrintWriter
import java.net.InetAddress

package object terminals {

  val akkaSystem: ActorSystem = ActorSystem("nj-devices")

  val cred = FtpCredentials.create("chenh", "test")

  val ftpSettins: FtpSettings =
    FtpSettings(InetAddress.getLocalHost)
      .withPort(21)
      .withCredentials(cred)
      .withPassiveMode(true)
      .withConfigureConnection { (ftpClient: FTPClient) =>
        ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true))
        ftpClient.setRemoteVerificationEnabled(false)
      }
}
