package com.github.chenharryhua.nanjin.common

import cats.Semigroup

import java.net.InetAddress

trait HostName {
  def name: String
}
object HostName {
  implicit val semigroupHostName: Semigroup[HostName] = (x: HostName, y: HostName) =>
    new HostName {
      override val name: String = x.name + "/" + y.name
    }

  val local_host: HostName = new HostName {
    override val name: String = InetAddress.getLocalHost.getHostName
  }
}
