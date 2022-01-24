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

  def apply(spans: List[Span], serviceParams: ServiceParams): Digested = {
    val nel: NonEmptyList[Span] = spans match {
      case Nil          => NonEmptyList.of(Span("root"))
      case head :: tail => NonEmptyList(head, tail)
    }
    val digest: String =
      digestSpans(NonEmptyList.of(serviceParams.taskParams.appName, serviceParams.serviceName) ::: nel)
    new Digested(nel, digest)
  }

  def apply(serviceName: ServiceName, taskParams: TaskParams): Digested = {
    val nel: NonEmptyList[Span] = NonEmptyList.of(taskParams.appName, serviceName)
    new Digested(NonEmptyList.of(serviceName), digestSpans(nel))
  }
}
