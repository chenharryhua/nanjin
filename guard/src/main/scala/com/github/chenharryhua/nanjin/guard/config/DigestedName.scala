package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import io.circe.generic.JsonCodec

@JsonCodec
final case class DigestedName private (origin: String, digest: String) {
  val metricRepr: String = s"[$origin][$digest]"

  override def toString: String = metricRepr
}

private[guard] object DigestedName {
  private val LENGTH: Int = 8

  implicit val showDigestedName: Show[DigestedName] = _.metricRepr

  def apply(spans: List[Span], serviceParams: ServiceParams): DigestedName = {
    val name: String = spans.map(_.value).mkString("/")
    val sha1Hex: String =
      DigestUtils.sha1Hex(s"${serviceParams.taskParams.appName.value}/${serviceParams.serviceName.value}/$name")
    new DigestedName(name, sha1Hex.take(LENGTH))
  }

  def apply(serviceName: ServiceName, taskParams: TaskParams): DigestedName = {
    val sha1Hex: String = DigestUtils.sha1Hex(s"${taskParams.appName.value}/${serviceName.value}")
    new DigestedName(serviceName.value, sha1Hex.take(LENGTH))
  }
}
