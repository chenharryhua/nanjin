package com.github.chenharryhua.nanjin.kafka

import akka.NotUsed
import akka.stream.scaladsl.Sink
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import cats.effect.ConcurrentEffect
import cats.effect.concurrent.Deferred
import cats.implicits._

object akkaSinks {

  final private class IgnoreSink[F[_]](implicit F: ConcurrentEffect[F])
      extends GraphStageWithMaterializedValue[SinkShape[Any], F[NotUsed]] {

    val in: Inlet[Any]        = Inlet[Any]("Ignore.in")
    val shape: SinkShape[Any] = SinkShape(in)

    override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes): (GraphStageLogic, F[NotUsed]) = {
      val promise = Deferred.unsafe[F, Either[Throwable, NotUsed]]
      val logic = new GraphStageLogic(shape) with InHandler {

        override def preStart(): Unit = pull(in)
        override def onPush(): Unit   = pull(in)

        override def onUpstreamFinish(): Unit = {
          super.onUpstreamFinish()
          F.toIO(promise.complete(Right(NotUsed))).unsafeRunSync()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          super.onUpstreamFailure(ex)
          F.toIO(promise.complete(Left(ex))).unsafeRunSync()
        }
        setHandler(in, this)
      }

      (logic, promise.get.rethrow)
    }
  }
  def ignore[F[_]: ConcurrentEffect]: Sink[Any, F[NotUsed]] = Sink.fromGraph(new IgnoreSink[F])
}
