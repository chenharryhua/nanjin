package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import org.apache.commons.codec.digest.DigestUtils
import io.circe.generic.JsonCodec

@JsonCodec
final case class MeasurementName private (value: String, digest: String) {
  override val toString: String = s"[$digest][$value]"
}

object MeasurementName {
  implicit val showMeasurementID: Show[MeasurementName] = Show.fromToString

  def apply(serviceParams: ServiceParams, name: String): MeasurementName = {
    val withPrefix = serviceParams.metricParams.namePrefix + name
    val fullName: List[String] =
      serviceParams.taskParams.taskName.value :: serviceParams.serviceName.value :: withPrefix :: Nil
    MeasurementName(withPrefix, DigestUtils.sha1Hex(fullName.mkString("/")).take(8))
  }
}
