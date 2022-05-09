package com.github.chenharryhua.nanjin.common

import cats.Show
import cats.kernel.Monoid
import com.github.chenharryhua.nanjin.common.HostName.monoidHostName
import io.circe.generic.JsonCodec

import java.net.InetAddress
import scala.util.Try

@JsonCodec
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
  implicit val showHostName: Show[HostName] = cats.derived.semiauto.show[HostName]
  implicit val monoidHostName: Monoid[HostName] = new Monoid[HostName] {
    override val empty: HostName = HostName("")

    override def combine(x: HostName, y: HostName): HostName = x / y
  }

  val local_host: HostName =
    HostName(Try(Option(InetAddress.getLocalHost.getHostName)).toOption.flatten.getOrElse("none"))
}
