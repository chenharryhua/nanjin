package com.github.chenharryhua.nanjin.kafka

import cats.effect.ConcurrentEffect
import com.github.chenharryhua.nanjin.codec._
import org.scalatest.funsuite.AnyFunSuite
import monix.eval.{Task, TaskApp}
import monix.eval.instances.CatsConcurrentEffectForTask
import monix.execution.Scheduler

class MonixTest extends AnyFunSuite {
  val scheduler: Scheduler  = Scheduler.global
  val options: Task.Options = Task.defaultOptions.withSchedulerFeatures(scheduler)
  implicit lazy val catsEffect: ConcurrentEffect[Task] =
    new CatsConcurrentEffectForTask()(scheduler, options)
  val ctx: MonixKafkaContext = KafkaSettings.local.monixContext

}
