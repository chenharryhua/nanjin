package com.github.chenharryhua.nanjin.common

import cats.Show
import cats.kernel.Monoid
import com.github.chenharryhua.nanjin.common.HostName.monoidHostName
import io.circe.{Decoder, Encoder}

import java.net.InetAddress
import scala.util.Try

final case class HostName(value: String) extends AnyVal {
  def /(other: HostName): HostName =
    (value.isEmpty, other.value.isEmpty) match {
      case (true, true)   => monoidHostName.empty
      case (false, true)  => this
      case (true, false)  => other
      case (false, false) => HostName(s"$value/${other.value}")
    }
}

object HostName {
  implicit final val showHostName: Show[HostName] = cats.derived.semiauto.show[HostName]
  implicit final val monoidHostName: Monoid[HostName] = new Monoid[HostName] {
    override val empty: HostName = HostName("")

    override def combine(x: HostName, y: HostName): HostName = x / y
  }

  implicit final val encoderHostName: Encoder[HostName] = Encoder.encodeString.contramap(_.value)
  implicit final val decoderHostName: Decoder[HostName] = Decoder.decodeString.map(HostName(_))

  val local_host: HostName =
    HostName(Try(Option(InetAddress.getLocalHost.getHostName)).toOption.flatten.getOrElse("none"))
}
