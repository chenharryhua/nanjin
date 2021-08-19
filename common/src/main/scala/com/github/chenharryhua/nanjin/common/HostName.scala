package com.github.chenharryhua.nanjin.common

import cats.Semigroup

import java.net.InetAddress
import scala.util.Try

trait HostName {
  def name: String
}
object HostName {
  implicit val semigroupHostName: Semigroup[HostName] = (x: HostName, y: HostName) =>
    new HostName {
      override val name: String = x.name + "/" + y.name
    }

  val local_host: HostName = new HostName {
    override val name: String = Try(InetAddress.getLocalHost.getHostName).getOrElse("none")
  }
}
