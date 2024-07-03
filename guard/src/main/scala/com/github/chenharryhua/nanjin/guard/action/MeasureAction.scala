package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Unique
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.{CounterKind, TimerKind}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/** TimeUnit.NANOSECONDS.toNanos((fd1:FiniteDuration - fd2:FiniteDuration).toNano)
  *
  * slightly cost more than
  *
  * (fd1:FiniteDuration - fd2:FiniteDuration).toJava.toNano
  */

sealed private trait MeasureAction {
  def done(fd: FiniteDuration): Unit
  def fail(): Unit
  def count_retry(): Unit
  def unregister(): Unit
}

private object MeasureAction {
  def apply(
    actionParams: ActionParams,
    metricRegistry: MetricRegistry,
    token: Unique.Token): MeasureAction = {
    val metricName: MetricName = actionParams.metricName

    val doneID: String =
      MetricID(metricName, Category.Counter(CounterKind.ActionDone), token).identifier
    val failID: String =
      MetricID(metricName, Category.Counter(CounterKind.ActionFail), token).identifier
    val retryID: String =
      MetricID(metricName, Category.Counter(CounterKind.ActionRetry), token).identifier

    val doneTimerID: String =
      MetricID(metricName, Category.Timer(TimerKind.Action), token).identifier

    (actionParams.isCounting, actionParams.isTiming) match {
      case (true, true) =>
        new MeasureAction {
          private[this] lazy val failC  = metricRegistry.counter(failID)
          private[this] lazy val doneC  = metricRegistry.counter(doneID)
          private[this] lazy val retryC = metricRegistry.counter(retryID)
          private[this] lazy val doneT  = metricRegistry.timer(doneTimerID)

          override def done(fd: FiniteDuration): Unit = {
            doneC.inc(1)
            doneT.update(fd.toNanos, TimeUnit.NANOSECONDS)
          }
          override def fail(): Unit        = failC.inc(1)
          override def count_retry(): Unit = retryC.inc(1)

          override def unregister(): Unit = {
            metricRegistry.remove(failID)
            metricRegistry.remove(doneID)
            metricRegistry.remove(retryID)
            metricRegistry.remove(doneTimerID)
            ()
          }
        }
      case (true, false) =>
        new MeasureAction {
          private[this] lazy val failC  = metricRegistry.counter(failID)
          private[this] lazy val doneC  = metricRegistry.counter(doneID)
          private[this] lazy val retryC = metricRegistry.counter(retryID)

          override def done(fd: FiniteDuration): Unit = doneC.inc(1)
          override def fail(): Unit                   = failC.inc(1)
          override def count_retry(): Unit            = retryC.inc(1)

          override def unregister(): Unit = {
            metricRegistry.remove(failID)
            metricRegistry.remove(doneID)
            metricRegistry.remove(retryID)
            ()
          }
        }
      case (false, true) =>
        new MeasureAction {
          private[this] lazy val doneT = metricRegistry.timer(doneTimerID)

          override def done(fd: FiniteDuration): Unit =
            doneT.update(fd.toNanos, TimeUnit.NANOSECONDS)
          override def fail(): Unit        = ()
          override def count_retry(): Unit = ()

          override def unregister(): Unit = {
            metricRegistry.remove(doneTimerID)
            ()
          }
        }

      case (false, false) =>
        new MeasureAction {
          override def done(fd: FiniteDuration): Unit = ()
          override def fail(): Unit                   = ()
          override def count_retry(): Unit            = ()
          override def unregister(): Unit             = ()
        }
    }
  }
}
