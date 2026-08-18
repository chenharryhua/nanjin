package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import org.scalatest.funsuite.AnyFunSuite

class AgentDomainTest extends AnyFunSuite {

  private val service = TaskGuard[IO]("domain").service("domain")

  test("1.withDomain propagates domain to ReportedEvent") {
    val events = service
      .eventStream { agent =>
        val scoped = agent.withDomain("my-domain")
        scoped.herald.info("hello from domain")
      }
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val reported = events.collect { case r: ReportedEvent => r }
    assert(reported.nonEmpty)
    assert(reported.head.domain.value == "my-domain")
  }

  test("2.default domain uses default value") {
    val events = service
      .eventStream { agent =>
        agent.herald.info("hello default")
      }
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val reported = events.collect { case r: ReportedEvent => r }
    assert(reported.nonEmpty)
    assert(reported.head.domain.value == "default")
  }

  test("3.multiple withDomain calls use their own domains") {
    val events = service
      .eventStream { agent =>
        val d1 = agent.withDomain("alpha")
        val d2 = agent.withDomain("beta")
        d1.herald.info("from alpha") *> d2.herald.info("from beta")
      }
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val reported = events.collect { case r: ReportedEvent => r }
    assert(reported.size == 2)
    assert(reported(0).domain.value == "alpha")
    assert(reported(1).domain.value == "beta")
  }

  test("4.withDomain does not affect original agent's domain") {
    val events = service
      .eventStream { agent =>
        val scoped = agent.withDomain("scoped")
        scoped.herald.info("scoped msg") *> agent.herald.info("original msg")
      }
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    val reported = events.collect { case r: ReportedEvent => r }
    assert(reported.size == 2)
    assert(reported(0).domain.value == "scoped")
    assert(reported(1).domain.value == "default")
  }
}
