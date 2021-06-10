package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.{ActionSucced, ServiceStopped}
import org.scalatest.funsuite.AnyFunSuite

class NameTest extends AnyFunSuite {
  test("should not change the names when update config") {
    val taskGuard: TaskGuard[IO] =
      TaskGuard[IO]("task.name").updateConfig(_.withHealthCheckDisabled).updateActionConfig(_.withSuccAlertOn)
    val serviceGuard = taskGuard.service("service.name").updateServiceConfig(_.withHealthCheckDisabled)
    val Vector(ActionSucced(_, a, _, _), ActionSucced(_, b, _, _), c) = serviceGuard.eventStream { ag =>
      ag("action.retry").updateActionConfig(_.withSuccAlertOn).retry(IO(1)).withSuccNotes((_, _) => "").run >>
        ag("action.retry.either")
          .updateActionConfig(_.withFailAlertOn)
          .retry(IO(Right("a")))
          .withFailNotes((_, _) => null)
          .run
    }.compile.toVector.unsafeRunSync()
    assert(a.serviceInfo.params.applicationName == "task.name")
    assert(a.serviceInfo.serviceName == "service.name")
    assert(a.actionName == "action.retry")
    assert(b.serviceInfo.params.applicationName == "task.name")
    assert(b.serviceInfo.serviceName == "service.name")
    assert(b.actionName == "action.retry.either")
    assert(c.isInstanceOf[ServiceStopped])
  }
}
