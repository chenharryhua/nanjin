package com.github.chenharryhua.nanjin.terminals

import akka.stream.*
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.*
import cats.Eval
import cats.effect.kernel.Sync
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}

import scala.concurrent.{Future, Promise}

object NJParquet {
  // input path may not exist when eval builder
  def fs2Source[F[_]](reader: F[ParquetReader[GenericRecord]])(implicit F: Sync[F]): Stream[F, GenericRecord] =
    for {
      rd <- Stream.bracket(reader)(r => F.blocking(r.close()))
      gr <- Stream.repeatEval(F.blocking(Option(rd.read()))).unNoneTerminate
    } yield gr

  def fs2Source[F[_]](reader: ParquetReader[GenericRecord])(implicit F: Sync[F]): Stream[F, GenericRecord] =
    fs2Source[F](F.pure(reader))

  def fs2Sink[F[_]](writer: ParquetWriter[GenericRecord])(implicit F: Sync[F]): Pipe[F, GenericRecord, Unit] = {
    def go(grs: Stream[F, GenericRecord], pw: ParquetWriter[GenericRecord]): Pull[F, Unit, Unit] =
      grs.pull.uncons.flatMap {
        case Some((hl, tl)) => Pull.eval(F.blocking(hl.foreach(pw.write))) >> go(tl, pw)
        case None           => Pull.done
      }

    (ss: Stream[F, GenericRecord]) =>
      for {
        pw <- Stream.bracket(F.pure(writer))(r => F.blocking(r.close()))
        _ <- go(ss, pw).stream
      } yield ()
  }

  def akkaSource(reader: Eval[ParquetReader[GenericRecord]]): Source[GenericRecord, Future[IOResult]] =
    Source.fromGraph(new ParquetSource(reader))

  def akkaSource(reader: ParquetReader[GenericRecord]): Source[GenericRecord, Future[IOResult]] =
    akkaSource(Eval.now(reader))

  def akkaSink(writer: ParquetWriter[GenericRecord]): Sink[GenericRecord, Future[IOResult]] =
    Sink.fromGraph(new ParquetSink(writer))
}

private class ParquetSource(parquetReader: Eval[ParquetReader[GenericRecord]])
    extends GraphStageWithMaterializedValue[SourceShape[GenericRecord], Future[IOResult]] {

  private val out: Outlet[GenericRecord] = Outlet("akka.parquet.source")

  override protected val initialAttributes: Attributes = super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[IOResult]) = {
    val promise: Promise[IOResult] = Promise[IOResult]()
    val logic = new GraphStageLogicWithLogging(shape) {
      override protected val logSource: Class[ParquetSource] = classOf[ParquetSource]
      setHandler(
        out,
        new OutHandler {
          private var count: Long = 0

          private val reader: ParquetReader[GenericRecord] = parquetReader.value

          override def onDownstreamFinish(cause: Throwable): Unit =
            try {
              super.onDownstreamFinish(cause)
              reader.close()
              cause match {
                case _: SubscriptionWithCancelException.NonFailureCancellation =>
                  promise.success(IOResult(count))
                case ex: Throwable =>
                  promise.failure(new IOOperationIncompleteException("parquet.source", count, ex))
              }
            } catch {
              case ex: Throwable => promise.failure(ex)
            }

          override def onPull(): Unit = {
            val record = reader.read()
            count += 1
            Option(record).fold(complete(out))(push(out, _))
          }
        }
      )
    }
    (logic, promise.future)
  }

  override val shape: SourceShape[GenericRecord] = SourceShape.of(out)
}

private class ParquetSink(writer: ParquetWriter[GenericRecord])
    extends GraphStageWithMaterializedValue[SinkShape[GenericRecord], Future[IOResult]] {

  private val in: Inlet[GenericRecord] = Inlet("akka.parquet.sink")

  override val shape: SinkShape[GenericRecord] = SinkShape.of(in)

  override protected val initialAttributes: Attributes = super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(attr: Attributes): (GraphStageLogic, Future[IOResult]) = {
    val promise: Promise[IOResult] = Promise[IOResult]()
    val logic = new GraphStageLogicWithLogging(shape) {
      override protected val logSource: Class[ParquetSink] = classOf[ParquetSink]
      setHandler(
        in,
        new InHandler {
          private var count: Long = 0

          override def onUpstreamFinish(): Unit =
            try {
              super.onUpstreamFinish()
              writer.close()
              promise.success(IOResult(count))
            } catch {
              case ex: Throwable => promise.failure(ex)
            }

          override def onUpstreamFailure(ex: Throwable): Unit =
            try {
              super.onUpstreamFailure(ex)
              writer.close()
              promise.failure(new IOOperationIncompleteException("parquet.sink", count, ex))
            } catch {
              case ex: Throwable => promise.failure(ex)
            }
          override def onPush(): Unit = {
            val gr: GenericRecord = grab(in)
            count += 1
            writer.write(gr)
            pull(in)
          }
        }
      )
      override def preStart(): Unit = pull(in)
    }
    (logic, promise.future)
  }
}
