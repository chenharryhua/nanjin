package mtest.kafka

import cats.syntax.show.given
import com.github.chenharryhua.nanjin.kafka.streaming.{
  KafkaStreamsAbnormallyStopped,
  KafkaStreamsStartupTimeout,
  StateTransition
}
import io.circe.syntax.given
import org.apache.kafka.streams.KafkaStreams.State
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class KafkaStreamsPureTest extends AnyFunSuite {

  test("1.StateTransition toString") {
    val st = StateTransition("my-app", State.REBALANCING, State.RUNNING)
    assert(st.toString === "StateTransition(application.id=my-app, REBALANCING ==> RUNNING)")
  }

  test("2.StateTransition Show instance") {
    val st = StateTransition("my-app", State.CREATED, State.REBALANCING)
    assert(st.show === "StateTransition(application.id=my-app, CREATED ==> REBALANCING)")
  }

  test("3.StateTransition Encoder produces correct JSON") {
    val st = StateTransition("my-app", State.RUNNING, State.PENDING_SHUTDOWN)
    val json = st.asJson
    assert(json.hcursor.get[String]("event").toOption.contains("KafkaStreams.State.Transition"))
    assert(json.hcursor.get[String]("applicationId").toOption.contains("my-app"))
    assert(json.hcursor.get[String]("oldState").toOption.contains("RUNNING"))
    assert(json.hcursor.get[String]("newState").toOption.contains("PENDING_SHUTDOWN"))
  }

  test("4.KafkaStreamsAbnormallyStopped message") {
    val ex = KafkaStreamsAbnormallyStopped("my-app")
    assert(ex.getMessage === "KafkaStreams(my-app) were stopped abnormally")
    assert(ex.getStackTrace.isEmpty) // NoStackTrace
  }

  test("5.KafkaStreamsStartupTimeout message") {
    val ex = KafkaStreamsStartupTimeout("my-app", 30.seconds)
    assert(ex.getMessage === "KafkaStreams(my-app) did not reach RUNNING within 30 seconds")
    assert(ex.getStackTrace.isEmpty) // NoStackTrace
  }
}
