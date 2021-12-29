package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import io.circe.generic.JsonCodec

@JsonCodec
final case class MetricName private (name: String, digest: String) {
  val value: String = s"$name/$digest"
}

private[guard] object MetricName {
  private val LENGTH: Int = 8

  implicit val showMetricName: Show[MetricName] = _.value

  def apply(spans: List[String], serviceParams: ServiceParams): MetricName = {
    val name: String    = spans.mkString("/")
    val sha1Hex: String = DigestUtils.sha1Hex(s"${serviceParams.taskParams.appName}/${serviceParams.serviceName}/$name")
    new MetricName(name, sha1Hex.take(LENGTH))
  }

  def apply(serviceName: String, taskParams: TaskParams): MetricName = {
    val sha1Hex: String = DigestUtils.sha1Hex(s"${taskParams.appName}/$serviceName")
    new MetricName(serviceName, sha1Hex.take(LENGTH))
  }
}
