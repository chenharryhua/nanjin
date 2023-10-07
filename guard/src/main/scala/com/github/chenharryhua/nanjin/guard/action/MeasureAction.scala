package com.github.chenharryhua.nanjin.guard.action

import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.{
  ActionParams,
  Category,
  CounterKind,
  MetricID,
  MetricName,
  TimerKind
}

import java.time.Duration

/** TimeUnit.NANOSECONDS.toNanos((fd1:FiniteDuration - fd2:FiniteDuration).toNano)
  *
  * slightly cost more than
  *
  * (fd1:FiniteDuration - fd2:FiniteDuration).toJava.toNano
  */

sealed private trait MeasureAction {
  def done(fd: => Duration): Unit
  def fail(fd: => Duration): Unit
  def countRetry(): Unit
  def unregister(): Unit
}

private object MeasureAction {
  def apply(actionParams: ActionParams, metricRegistry: MetricRegistry): MeasureAction = {
    val metricName: MetricName = actionParams.metricName
    val doneID: String         = MetricID(metricName, Category.Counter(CounterKind.ActionDone)).identifier
    val failID: String         = MetricID(metricName, Category.Counter(CounterKind.ActionFail)).identifier
    val retryID: String        = MetricID(metricName, Category.Counter(CounterKind.ActionRetry)).identifier

    val doneTimerID: String =
      MetricID(metricName, Category.Timer(TimerKind.ActionDoneTimer)).identifier
    val failTimerID: String =
      MetricID(metricName, Category.Timer(TimerKind.ActionFailTimer)).identifier

    (actionParams.isCounting, actionParams.isTiming) match {
      case (true, true) =>
        new MeasureAction {
          private lazy val failC  = metricRegistry.counter(failID)
          private lazy val doneC  = metricRegistry.counter(doneID)
          private lazy val retryC = metricRegistry.counter(retryID)
          private lazy val doneT  = metricRegistry.timer(doneTimerID)
          private lazy val failT  = metricRegistry.timer(failTimerID)

          override def done(fd: => Duration): Unit = {
            doneC.inc(1)
            doneT.update(fd)
          }
          override def fail(fd: => Duration): Unit = {
            failC.inc(1)
            failT.update(fd)
          }
          override def countRetry(): Unit = retryC.inc(1)

          override def unregister(): Unit = {
            metricRegistry.remove(failID)
            metricRegistry.remove(doneID)
            metricRegistry.remove(retryID)
            metricRegistry.remove(doneTimerID)
            metricRegistry.remove(failTimerID)
            ()
          }
        }
      case (true, false) =>
        new MeasureAction {
          private lazy val failC  = metricRegistry.counter(failID)
          private lazy val doneC  = metricRegistry.counter(doneID)
          private lazy val retryC = metricRegistry.counter(retryID)

          override def done(fd: => Duration): Unit = doneC.inc(1)
          override def fail(fd: => Duration): Unit = failC.inc(1)
          override def countRetry(): Unit          = retryC.inc(1)

          override def unregister(): Unit = {
            metricRegistry.remove(failID)
            metricRegistry.remove(doneID)
            metricRegistry.remove(retryID)
            ()
          }
        }
      case (false, true) =>
        new MeasureAction {
          private lazy val doneT = metricRegistry.timer(doneTimerID)
          private lazy val failT = metricRegistry.timer(failTimerID)

          override def done(fd: => Duration): Unit = doneT.update(fd)
          override def fail(fd: => Duration): Unit = failT.update(fd)
          override def countRetry(): Unit          = ()

          override def unregister(): Unit = {
            metricRegistry.remove(doneTimerID)
            metricRegistry.remove(failTimerID)
            ()
          }
        }

      case (false, false) =>
        new MeasureAction {
          override def done(fd: => Duration): Unit = ()
          override def fail(fd: => Duration): Unit = ()
          override def countRetry(): Unit          = ()
          override def unregister(): Unit          = ()
        }
    }
  }
}
