package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import io.circe.generic.JsonCodec

@JsonCodec
final case class MetricName private (value: String) extends AnyVal
object MetricName {
  implicit val showMetricName: Show[MetricName] = cats.derived.semiauto.show[MetricName]
  def apply(spans: List[String], serviceParams: ServiceParams): MetricName = {
    val name: String    = spans.mkString("/")
    val sha1Hex: String = DigestUtils.sha1Hex(s"${serviceParams.taskParams.appName}/${serviceParams.serviceName}/$name")
    MetricName(s"$name/${sha1Hex.take(8)}")
  }
  def apply(serviceParams: ServiceParams): MetricName = {
    val sha1Hex = DigestUtils.sha1Hex(s"${serviceParams.taskParams.appName}/${serviceParams.serviceName}")
    MetricName(s"${serviceParams.serviceName}/${sha1Hex.take(8)}")
  }
}
