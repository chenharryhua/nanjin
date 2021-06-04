package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.{ActionSucced, ServiceStoppedAbnormally}
import org.scalatest.funsuite.AnyFunSuite

class NameTest extends AnyFunSuite {
  test("should not change the names when update config") {
    val taskGuard: TaskGuard[IO] =
      TaskGuard[IO]("task.name").updateServiceConfig(_.withHealthCheckDisabled).updateActionConfig(_.withSuccAlertOn)
    val serviceGuard = taskGuard.service("service.name").updateServiceConfig(_.withHealthCheckDisabled)
    val Vector(ActionSucced(a, _, _, _), ActionSucced(b, _, _, _), c) = serviceGuard.eventStream { ag =>
      ag("action.retry").updateActionConfig(_.withSuccAlertOn).retry(IO(1)).withSuccNotes((_, _) => "").run >>
        ag("action.retry.either")
          .updateActionConfig(_.withFailAlertOn)
          .retry(IO(Right("a")))
          .withFailNotes((_, _) => null)
          .run
    }.compile.toVector.unsafeRunSync()
    assert(a.appName == "task.name")
    assert(a.serviceName == "service.name")
    assert(a.actionName == "action.retry")
    assert(b.appName == "task.name")
    assert(b.serviceName == "service.name")
    assert(b.actionName == "action.retry.either")
    assert(c.isInstanceOf[ServiceStoppedAbnormally])
  }
}
