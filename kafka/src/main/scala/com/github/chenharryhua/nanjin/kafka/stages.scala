package com.github.chenharryhua.nanjin.kafka

import akka.stream._
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.stage._
import akka.{Done, NotUsed}
import cats.effect.ConcurrentEffect
import cats.effect.concurrent.Deferred
import cats.syntax.all._
import fs2.kafka.KafkaByteConsumerRecord
import org.apache.kafka.common.TopicPartition

object stages {

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

  final private class KafkaTakeUntilEnd(endOffsets: KafkaTopicPartition[Long])
      extends GraphStage[FlowShape[KafkaByteConsumerRecord, KafkaByteConsumerRecord]] {
    val in: Inlet[KafkaByteConsumerRecord]   = Inlet[KafkaByteConsumerRecord]("kafka.take.until.end")
    val out: Outlet[KafkaByteConsumerRecord] = Outlet[KafkaByteConsumerRecord]("kafka.take.until.end")

    override def shape: FlowShape[KafkaByteConsumerRecord, KafkaByteConsumerRecord] = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      private var topicStates: Map[TopicPartition, Boolean] = endOffsets.value.mapValues(_ => false)

      private def isAllTopicCompleted: Boolean = topicStates.forall(_._2)

      setHandlers(
        in,
        out,
        new InHandler with OutHandler {

          override def onPush(): Unit = {
            val cr: KafkaByteConsumerRecord = grab(in)
            val tp: TopicPartition          = new TopicPartition(cr.topic(), cr.partition())
            val offset: Option[Long]        = endOffsets.get(tp)
            if (offset.exists(cr.offset() < _)) {
              push(out, cr)
            } else if (offset.contains(cr.offset())) {
              push(out, cr)
              topicStates += tp -> true
              if (isAllTopicCompleted) completeStage()
            } else {
              topicStates += tp -> true
              if (isAllTopicCompleted) completeStage()
            }
          }
          override def onPull(): Unit = if (isAllTopicCompleted) completeStage() else pull(in)
        }
      )
    }
  }

  def takeUntilEnd(
    endOffsets: KafkaTopicPartition[Long]): Flow[KafkaByteConsumerRecord, KafkaByteConsumerRecord, NotUsed] =
    Flow.fromGraph(new KafkaTakeUntilEnd(endOffsets))

}
