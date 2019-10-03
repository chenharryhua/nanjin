package mtest

import cats.effect.ConcurrentEffect
import com.github.chenharryhua.nanjin.kafka.{KafkaSettings, MonixKafkaContext}
import monix.eval.Task
import monix.eval.instances.CatsConcurrentEffectForTask
import monix.execution.Scheduler
import org.scalatest.funsuite.AnyFunSuite

class MonixTest extends AnyFunSuite {
  val scheduler: Scheduler  = Scheduler.global
  val options: Task.Options = Task.defaultOptions.withSchedulerFeatures(scheduler)

  implicit lazy val catsEffect: ConcurrentEffect[Task] =
    new CatsConcurrentEffectForTask()(scheduler, options)
  val ctx: MonixKafkaContext = KafkaSettings.local.monixContext

}
