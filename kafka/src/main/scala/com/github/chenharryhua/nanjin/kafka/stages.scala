package com.github.chenharryhua.nanjin.kafka

import akka.stream.*
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.stage.*
import akka.{Done, NotUsed}
import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import fs2.kafka.KafkaByteConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.util.control.NonFatal

object stages {

  /** Notes:
    *
    * similar to akka.stream.scaladsl.Sink.ignore: A Sink that will consume the stream and discard the elements. it's
    * materialized to '''F[Done]'''instead of '''Future[Done]'''
    */
  final private class IgnoreSink[F[_]: Async](dispatcher: Dispatcher[F])
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
          dispatcher.unsafeRunSync(promise.complete(Right(Done)).void)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          super.onUpstreamFailure(ex)
          dispatcher.unsafeRunSync(promise.complete(Left(ex)).void)
        }
        setHandler(in, this)
      }

      (logic, promise.get.rethrow)
    }
  }
  def ignore[F[_]: Async](dispatcher: Dispatcher[F]): Sink[Any, F[Done]] = Sink.fromGraph(new IgnoreSink[F](dispatcher))

  /** Notes:
    *
    * @param endOffsets
    *   end offsets of all partitions
    *
    * '''Emits''' when offset of the record is less than the end offset
    *
    * '''Completes''' when all partitions reach their end offsets
    *
    * '''Cancels''' when downstream cancels
    */
  final private class KafkaTakeUntilEnd(endOffsets: KafkaTopicPartition[KafkaOffset])
      extends GraphStage[FlowShape[KafkaByteConsumerRecord, KafkaByteConsumerRecord]] {
    val in: Inlet[KafkaByteConsumerRecord]   = Inlet[KafkaByteConsumerRecord]("kafka.take.until.end")
    val out: Outlet[KafkaByteConsumerRecord] = Outlet[KafkaByteConsumerRecord]("kafka.take.until.end")

    override def shape: FlowShape[KafkaByteConsumerRecord, KafkaByteConsumerRecord] = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      /** Notes:
        *
        * '''false''' end offset is not reached yet
        *
        * '''true''' end offset has been reached
        */
      private var topicStates: Map[TopicPartition, Boolean] = endOffsets.value.view.mapValues(_ => false).toMap

      /** Notes:
        *
        * '''true''' all partitions reach end offset
        *
        * '''false''' not all partition reach end offset
        */
      private def isPartitionsCompleted(ts: Map[TopicPartition, Boolean]): Boolean = ts.forall(_._2)

      private def decider: Decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      setHandlers(
        in,
        out,
        new InHandler with OutHandler {

          override def onPush(): Unit = {
            val cr: KafkaByteConsumerRecord = grab(in)
            val tp: TopicPartition          = new TopicPartition(cr.topic(), cr.partition())
            val offset: Option[Long]        = endOffsets.get(tp).map(_.offset.value - 1)
            try
              if (offset.exists(cr.offset() < _)) {
                push(out, cr)
              } else if (offset.contains(cr.offset())) {
                push(out, cr)
                topicStates += tp -> true
                if (isPartitionsCompleted(topicStates)) completeStage()
              } else {
                topicStates += tp -> true
                if (isPartitionsCompleted(topicStates)) completeStage() else pull(in)
              }
            catch {
              case NonFatal(ex) =>
                decider(ex) match {
                  case Supervision.Stop => failStage(ex)
                  case _                => pull(in)
                }
            }
          }
          override def onPull(): Unit = pull(in)
        }
      )
    }
  }

  def takeUntilEnd(
    endOffsets: KafkaTopicPartition[KafkaOffset]): Flow[KafkaByteConsumerRecord, KafkaByteConsumerRecord, NotUsed] =
    Flow.fromGraph(new KafkaTakeUntilEnd(endOffsets))

}
