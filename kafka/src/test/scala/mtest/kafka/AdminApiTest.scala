package mtest.kafka

import cats.effect.IO
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class AdminApiTest extends AnyFunSuite {
  val topic  = ctx.topic[Int, Int]("admin")
  val mirror = ctx.topic[Int, Int]("admin.mirror")

  test("newTopic") {
    val run = for {
      d <- topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- IO.sleep(1.seconds)
      n <- topic.admin.newTopic(3, 1)
      _ <- IO.sleep(1.seconds)
      info <- topic.admin.describe
    } yield ()
    run.unsafeRunSync()

  }
  test("mirrorTo") {
    val run = for {
      d <- mirror.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- IO.sleep(1.seconds)
      n <- topic.admin.mirrorTo(mirror.topicName, 1)
      _ <- IO.sleep(1.seconds)
      info <- topic.admin.describe
    } yield ()
    run.unsafeRunSync()
  }

  test("groups") {
    topic.admin.groups.unsafeRunSync()
  }
}
