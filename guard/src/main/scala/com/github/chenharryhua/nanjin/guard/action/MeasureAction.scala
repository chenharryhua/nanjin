package com.github.chenharryhua.nanjin.guard.action

import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Category, CounterKind, MetricID, MetricName}
import io.circe.syntax.EncoderOps

import java.time.Duration

sealed private trait MeasureAction {
  def done(fd: => Duration): Unit
  def fail(fd: => Duration): Unit
  def countRetry(): Unit
}
private object MeasureAction {
  def apply(actionParams: ActionParams, metricRegistry: MetricRegistry): MeasureAction = {
    val metricName: MetricName     = actionParams.metricId.metricName
    val doneCat: Category.Counter  = Category.Counter(CounterKind.ActionDone)
    val failCat: Category.Counter  = Category.Counter(CounterKind.ActionFail)
    val retryCat: Category.Counter = Category.Counter(CounterKind.ActionRetry)

    (actionParams.isCounting, actionParams.isTiming) match {
      case (true, true) =>
        new MeasureAction {
          private lazy val failC  = metricRegistry.counter(MetricID(metricName, failCat).asJson.noSpaces)
          private lazy val doneC  = metricRegistry.counter(MetricID(metricName, doneCat).asJson.noSpaces)
          private lazy val retryC = metricRegistry.counter(MetricID(metricName, retryCat).asJson.noSpaces)
          private lazy val timer  = metricRegistry.timer(actionParams.metricId.asJson.noSpaces)

          override def done(fd: => Duration): Unit = {
            doneC.inc(1)
            timer.update(fd)
          }
          override def fail(fd: => Duration): Unit = {
            failC.inc(1)
            timer.update(fd)
          }
          override def countRetry(): Unit = retryC.inc(1)
        }
      case (true, false) =>
        new MeasureAction {
          private lazy val failC  = metricRegistry.counter(MetricID(metricName, failCat).asJson.noSpaces)
          private lazy val doneC  = metricRegistry.counter(MetricID(metricName, doneCat).asJson.noSpaces)
          private lazy val retryC = metricRegistry.counter(MetricID(metricName, retryCat).asJson.noSpaces)

          override def done(fd: => Duration): Unit = doneC.inc(1)
          override def fail(fd: => Duration): Unit = failC.inc(1)
          override def countRetry(): Unit          = retryC.inc(1)
        }
      case (false, true) =>
        new MeasureAction {
          private lazy val timer = metricRegistry.timer(actionParams.metricId.asJson.noSpaces)

          override def done(fd: => Duration): Unit = timer.update(fd)
          override def fail(fd: => Duration): Unit = timer.update(fd)
          override def countRetry(): Unit          = ()
        }

      case (false, false) =>
        new MeasureAction {
          override def done(fd: => Duration): Unit = ()
          override def fail(fd: => Duration): Unit = ()
          override def countRetry(): Unit          = ()
        }
    }
  }
}
