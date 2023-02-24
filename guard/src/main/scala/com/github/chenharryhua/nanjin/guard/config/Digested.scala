package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import io.circe.generic.JsonCodec

@JsonCodec
final case class Digested private (name: String, digest: String) {
  override val toString: String = s"[$name][$digest]"
}

object Digested {
  implicit val showDigested: Show[Digested] = Show.fromToString

  def apply(serviceParams: ServiceParams, name: String): Digested = {
    val fullName: List[String] =
      serviceParams.taskParams.taskName.value :: serviceParams.serviceName.value :: name :: Nil
    Digested(name, DigestUtils.sha1Hex(fullName.mkString("/")).take(8))
  }
}
