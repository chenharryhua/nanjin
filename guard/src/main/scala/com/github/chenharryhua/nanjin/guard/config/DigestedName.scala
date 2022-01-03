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

  def apply(spans: List[String], serviceParams: ServiceParams): DigestedName = {
    val name: String    = spans.mkString("/")
    val sha1Hex: String = DigestUtils.sha1Hex(s"${serviceParams.taskParams.appName}/${serviceParams.serviceName}/$name")
    new DigestedName(name, sha1Hex.take(LENGTH))
  }

  def apply(serviceName: String, taskParams: TaskParams): DigestedName = {
    val sha1Hex: String = DigestUtils.sha1Hex(s"${taskParams.appName}/$serviceName")
    new DigestedName(serviceName, sha1Hex.take(LENGTH))
  }
}
