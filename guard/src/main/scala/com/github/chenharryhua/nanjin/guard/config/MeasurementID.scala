package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import org.apache.commons.codec.digest.DigestUtils
import io.circe.generic.JsonCodec

@JsonCodec
final case class MeasurementID private (name: String, digest: String) {
  override val toString: String = s"[$digest][$name]"
}

object MeasurementID {
  implicit val showMeasurementID: Show[MeasurementID] = Show.fromToString

  def apply(serviceParams: ServiceParams, name: String): MeasurementID = {
    val withPrefix = serviceParams.metricParams.namePrefix + name
    val fullName: List[String] =
      serviceParams.taskParams.taskName.value :: serviceParams.serviceName.value :: withPrefix :: Nil
    MeasurementID(withPrefix, DigestUtils.sha1Hex(fullName.mkString("/")).take(8))
  }
}
