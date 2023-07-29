package com.github.chenharryhua.nanjin.common

import cats.Show
import cats.kernel.Monoid
import com.github.chenharryhua.nanjin.common.HostName.monoidHostName
import io.circe.{Decoder, Encoder}

import java.net.InetAddress
import scala.util.Try

final class HostName(val value: String) extends AnyVal with Serializable {
  def /(other: HostName): HostName =
    (value.isEmpty, other.value.isEmpty) match {
      case (true, true)   => monoidHostName.empty
      case (false, true)  => this
      case (true, false)  => other
      case (false, false) => new HostName(s"$value/${other.value}")
    }
}

object HostName {

  implicit final val showHostName: Show[HostName] = hn => s"HostName(value=${hn.value})"
  implicit final val monoidHostName: Monoid[HostName] = new Monoid[HostName] {
    override val empty: HostName = new HostName("")

    override def combine(x: HostName, y: HostName): HostName = x / y
  }

  implicit final val encoderHostName: Encoder[HostName] = Encoder.encodeString.contramap(_.value)
  implicit final val decoderHostName: Decoder[HostName] = Decoder.decodeString.map(new HostName(_))

  val local_host: HostName =
    new HostName(
      Try(Option(InetAddress.getLocalHost.getHostName)).toOption.flatten.getOrElse("local.host.none"))
}
