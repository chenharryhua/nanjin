package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.common.chrono.zones.berlinTime
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.translator.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class ConfigTest extends AnyFunSuite {
  val task: TaskGuard[IO] =
    TaskGuard[IO]("config")
      .updateConfig(_.withZoneId(berlinTime))
      .updateConfig(_.withMetricReport(policies.crontab(_.hourly)))

  test("1.counting") {
    val as = task
      .service("counting")
      .eventStream { agent =>
        agent.action("cfg", _.bipartite.counted).retry(IO(1)).buildWith(identity).use(_.run(()))
      }
      .map(checkJson)
      .filter(_.isInstanceOf[ActionStart])
      .compile
      .last
      .unsafeRunSync()
      .get
      .asInstanceOf[ActionStart]
    assert(as.actionParams.isCounting)
  }
  test("2.without counting") {
    val as = task
      .service("no count")
      .eventStream { agent =>
        agent.action("cfg", _.bipartite).retry(IO(1)).buildWith(identity).use(_.run(()))
      }
      .map(checkJson)
      .filter(_.isInstanceOf[ActionStart])
      .compile
      .last
      .unsafeRunSync()
      .get
      .asInstanceOf[ActionStart]
    assert(!as.actionParams.isCounting)
  }

  test("3.timing") {
    val as = task
      .service("timing")
      .eventStream { agent =>
        agent.action("cfg", _.bipartite.timed).retry(IO(1)).buildWith(identity).use(_.run(()))
      }
      .map(checkJson)
      .filter(_.isInstanceOf[ActionStart])
      .compile
      .last
      .unsafeRunSync()
      .get
      .asInstanceOf[ActionStart]
    assert(as.actionParams.isTiming)
  }

  test("4.without timing") {
    val as = task
      .service("no timing")
      .eventStream { agent =>
        agent.action("cfg", _.bipartite).retry(IO(1)).buildWith(identity).use(_.run(()))
      }
      .map(checkJson)
      .filter(_.isInstanceOf[ActionStart])
      .compile
      .last
      .unsafeRunSync()
      .get
      .asInstanceOf[ActionStart]
    assert(!as.actionParams.isTiming)
  }

  test("5.silent") {
    val as = task
      .service("silent")
      .eventStream { agent =>
        agent.action("cfg", _.silent).retry(IO(1)).buildWith(identity).use(_.run(()))
      }
      .map(checkJson)
      .filter(_.isInstanceOf[ActionStart])
      .compile
      .last
      .unsafeRunSync()
    assert(as.isEmpty)
  }

  test("6.report") {
    task
      .service("report")
      .updateConfig(_.withMetricReport(policies.giveUp))
      .eventStream { agent =>
        agent.action("cfg", _.silent).retry(IO(1)).buildWith(identity).use(_.run(()))
      }
      .map(checkJson)
      .filter(_.isInstanceOf[ServiceStart])
      .compile
      .last
      .unsafeRunSync()
  }

  test("7.reset") {
    task
      .service("reset")
      .eventStream { agent =>
        agent.action("cfg", _.silent).retry(IO(1)).buildWith(identity).use(_.run(()))
      }
      .map(checkJson)
      .filter(_.isInstanceOf[ServiceStart])
      .compile
      .last
      .unsafeRunSync()
  }

  test("8.composable action config") {
    val as = task
      .service("composable action")
      .eventStream(
        _.action("abc", _.bipartite.counted.timed)
          .delay(1)
          .buildWith(_.tapInput(_ => Json.Null)
            .tapOutput((_, _) => Json.Null)
            .tapError(_ => Json.Null)
            .worthRetry(_ => true))
          .use(_.run(())))
      .map(checkJson)
      .filter(_.isInstanceOf[ActionStart])
      .compile
      .last
      .unsafeRunSync()
      .get
      .asInstanceOf[ActionStart]

    assert(as.actionParams.isCounting)
    assert(as.actionParams.isTiming)
  }

  test("9.case") {
    val en = EventName.ServiceStart
    assert(en.entryName == "Service Start")
    assert(en.snake == "service_start")
    assert(en.compact == "ServiceStart")
    assert(en.camel == "serviceStart")
    assert(en.camelJson == Json.fromString("serviceStart"))
    assert(en.snakeJson == Json.fromString("service_start"))
    assert(en.compactJson == Json.fromString("ServiceStart"))
  }

  test("10.brief merge") {
    import io.circe.generic.auto.*
    final case class A(a: Int, z: Int)
    final case class B(b: Int, z: String)

    val ss = task
      .service("brief merge")
      .updateConfig(_.addBrief(A(1, 3).asJson))
      .updateConfig(_.addBrief(B(2, "b")))
      .updateConfig(_.addBrief(Json.Null))
      .updateConfig(_.addBrief(IO(A(1, 3))))
      .eventStream(_.action("cfg").retry(IO(1)).buildWith(identity).use(_.run(())))
      .map(checkJson)
      .filter(_.isInstanceOf[ServiceStart])
      .compile
      .last
      .unsafeRunSync()
      .get
      .asInstanceOf[ServiceStart]
    val ab = ss.serviceParams.brief.noSpaces
    assert(ab === """[{"a":1,"z":3},{"b":2,"z":"b"}]""")
  }
}
