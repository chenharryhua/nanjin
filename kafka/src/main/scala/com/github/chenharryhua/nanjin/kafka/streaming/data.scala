package com.github.chenharryhua.nanjin.kafka.streaming

import cats.Show
import io.circe.{Encoder, Json}
import org.apache.kafka.streams.KafkaStreams.State

import java.util.Properties
import scala.concurrent.duration.Duration
import scala.util.control.NoStackTrace

final case class KafkaStreamsAbnormallyStopped(applicationId: String)
    extends RuntimeException(s"KafkaStreams($applicationId) were stopped abnormally") with NoStackTrace

final case class KafkaStreamsStartupTimeout(applicationId: String, startupTimeout: Duration)
    extends RuntimeException(s"KafkaStreams($applicationId) did not reach RUNNING within $startupTimeout")
    with NoStackTrace

final case class StateTransition(applicationId: String, oldState: State, newState: State) {
  override def toString: String =
    s"StateTransition(application.id=$applicationId, ${oldState.name()} ==> ${newState.name()})"
}

object StateTransition {
  given Show[StateTransition] = Show.fromToString

  given Encoder[StateTransition] = (a: StateTransition) =>
    Json.obj(
      "event" -> Json.fromString("KafkaStreams.State.Transition"),
      "applicationId" -> Json.fromString(a.applicationId),
      "oldState" -> Json.fromString(a.oldState.name()),
      "newState" -> Json.fromString(a.newState.name())
    )
}

private def toProperties(props: Map[String, String]): Properties = {
  val p = new Properties()
  props.foreach { case (k, v) => p.setProperty(k, v) }
  p
}
