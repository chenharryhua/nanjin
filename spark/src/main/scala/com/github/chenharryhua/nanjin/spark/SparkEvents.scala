package com.github.chenharryhua.nanjin.spark

import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import fs2.concurrent.Channel
import fs2.Stream
import org.apache.spark.scheduler.*
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

final private class SparkEvents[F[_]](bus: Channel[F, SparkListenerEvent], dispatcher: Dispatcher[F])
    extends SparkListener {

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onTaskStart(event: SparkListenerTaskStart): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onJobStart(event: SparkListenerJobStart): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onJobEnd(event: SparkListenerJobEnd): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onStageExecutorMetrics(event: SparkListenerStageExecutorMetrics): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onExecutorExcluded(event: SparkListenerExecutorExcluded): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onExecutorExcludedForStage(event: SparkListenerExecutorExcludedForStage): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onNodeExcludedForStage(event: SparkListenerNodeExcludedForStage): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onExecutorUnexcluded(event: SparkListenerExecutorUnexcluded): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onNodeExcluded(event: SparkListenerNodeExcluded): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onNodeUnexcluded(event: SparkListenerNodeUnexcluded): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onUnschedulableTaskSetAdded(event: SparkListenerUnschedulableTaskSetAdded): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onUnschedulableTaskSetRemoved(event: SparkListenerUnschedulableTaskSetRemoved): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onSpeculativeTaskSubmitted(event: SparkListenerSpeculativeTaskSubmitted): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onOtherEvent(event: SparkListenerEvent): Unit = dispatcher.unsafeRunAndForget(bus.send(event))

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

}

object SparkEvents {
  def apply[F[_]](sparkContext: SparkContext)(implicit F: Async[F]): Stream[F, SparkListenerEvent] =
    for {
      bus <- Stream.eval(Channel.unbounded[F, SparkListenerEvent])
      dispatcher <- Stream.resource(Dispatcher.sequential[F])
      _ <- Stream.bracket {
        F.blocking {
          val listener = new SparkEvents(bus, dispatcher)
          sparkContext.addSparkListener(listener)
          listener
        }
      }(listener => F.blocking(sparkContext.removeSparkListener(listener)))
      event <- bus.stream
    } yield event

  def apply[F[_]: Async](sparkSession: SparkSession): Stream[F, SparkListenerEvent] =
    apply[F](sparkSession.sparkContext)
}
