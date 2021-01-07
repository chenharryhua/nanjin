package com.github.chenharryhua.nanjin.kafka

import akka.Done
import akka.stream.scaladsl.Sink
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import cats.effect.ConcurrentEffect
import cats.effect.concurrent.Deferred
import cats.syntax.all._

object akkaSinks {

  final private class IgnoreSink[F[_]](implicit F: ConcurrentEffect[F])
      extends GraphStageWithMaterializedValue[SinkShape[Any], F[Done]] {

    val in: Inlet[Any]        = Inlet[Any]("Ignore.in")
    val shape: SinkShape[Any] = SinkShape(in)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, F[Done]) = {
      val promise = Deferred.unsafe[F, Either[Throwable, Done]]
      val logic = new GraphStageLogic(shape) with InHandler {

        override def preStart(): Unit = pull(in)
        override def onPush(): Unit   = pull(in)

        override def onUpstreamFinish(): Unit = {
          super.onUpstreamFinish()
          F.toIO(promise.complete(Right(Done))).unsafeRunSync()
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
  def ignore[F[_]: ConcurrentEffect]: Sink[Any, F[Done]] = Sink.fromGraph(new IgnoreSink[F])
}
