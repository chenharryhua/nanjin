package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import io.circe.generic.JsonCodec

@JsonCodec
final case class Digested private (name: String, digest: String) {
  val metricRepr: String = s"[$name][$digest]"

  override val toString: String = metricRepr
}

object Digested {

  implicit val showDigestedName: Show[Digested] = _.metricRepr

  def apply(serviceParams: ServiceParams, name: String): Digested = {
    val fullName: List[String] =
      serviceParams.taskParams.taskName.value :: serviceParams.serviceName.value :: name :: Nil
    Digested(name, DigestUtils.sha1Hex(fullName.mkString("/")).take(8))
  }
}
