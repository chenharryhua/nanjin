package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import com.github.chenharryhua.nanjin.common.guard.Name
import io.circe.generic.JsonCodec
import io.circe.refined.*

@JsonCodec
final case class Digested private (spans: List[Name], digest: String) {
  val origin: String     = spans.map(_.value).toList.mkString("/")
  val metricRepr: String = s"[$origin][$digest]"

  override val toString: String = metricRepr
}

object Digested {

  implicit val showDigestedName: Show[Digested] = _.metricRepr

  def apply(serviceParams: ServiceParams, spans: List[Name]): Digested = {
    val fullSpan: List[String] =
      serviceParams.taskParams.taskName.value :: serviceParams.serviceName.value :: spans.map(_.value)
    Digested(spans, DigestUtils.sha1Hex(fullSpan.mkString("/")).take(8))
  }
}
