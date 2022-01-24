package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.data.NonEmptyList
import io.circe.generic.JsonCodec
import io.circe.refined.*

@JsonCodec
final case class Digested private (spans: NonEmptyList[Span], digest: String) {
  val origin: String     = spans.map(_.value).toList.mkString("/")
  val metricRepr: String = s"[$origin][$digest]"

  override val toString: String = metricRepr
}

private[guard] object Digested {

  implicit val showDigestedName: Show[Digested] = _.metricRepr

  def apply(spans: NonEmptyList[Span], serviceParams: ServiceParams): Digested = {
    val digest: String = digestSpans(NonEmptyList.one(serviceParams.taskParams.appName) ::: spans)
    new Digested(spans, digest)
  }

  def apply(serviceName: ServiceName, taskParams: TaskParams): Digested = {
    val nel: NonEmptyList[Span] = NonEmptyList.of(taskParams.appName, serviceName)
    new Digested(NonEmptyList.of(serviceName), digestSpans(nel))
  }
}
