package com.github.chenharryhua.nanjin.terminals

import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorAttributes, Attributes, Inlet, Outlet, SinkShape, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import cats.Eval
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.functor.*
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object NJParquet {
  // input path may not exist when eval builder
  def fs2Source[F[_]](builder: F[AvroParquetReader.Builder[GenericRecord]])(implicit
    F: Sync[F]): Stream[F, GenericRecord] =
    for {
      reader <- Stream.resource(Resource.make(builder.map(_.build()))(r => F.blocking(r.close())))
      gr <- Stream.repeatEval(F.delay(Option(reader.read()))).unNoneTerminate
    } yield gr

  def fs2Sink[F[_]](builder: AvroParquetWriter.Builder[GenericRecord])(implicit
    F: Sync[F]): Pipe[F, GenericRecord, Unit] = {
    def go(grs: Stream[F, GenericRecord], writer: ParquetWriter[GenericRecord]): Pull[F, Unit, Unit] =
      grs.pull.uncons.flatMap {
        case Some((hl, tl)) => Pull.eval(F.blocking(hl.foreach(writer.write))) >> go(tl, writer)
        case None           => Pull.eval(F.blocking(writer.close())) >> Pull.done
      }

    (ss: Stream[F, GenericRecord]) =>
      for {
        writer <- Stream.resource(Resource.make(F.blocking(builder.build()))(r => F.blocking(r.close())))
        _ <- go(ss, writer).stream
      } yield ()
  }

  def akkaSource(builder: Eval[AvroParquetReader.Builder[GenericRecord]]): Source[GenericRecord, NotUsed] =
    Source.fromGraph(new ParquetSource(builder))

  def akkaSink(builder: AvroParquetWriter.Builder[GenericRecord]): Sink[GenericRecord, Future[Done]] =
    Sink.fromGraph(new ParquetSink(builder))
}

private class ParquetSource[GenericRecord](builder: Eval[AvroParquetReader.Builder[GenericRecord]])
    extends GraphStage[SourceShape[GenericRecord]] {

  private val out: Outlet[GenericRecord] = Outlet("akka.parquet.source")

  override protected val initialAttributes: Attributes = super.initialAttributes.and(ActorAttributes.IODispatcher)

  private val reader: ParquetReader[GenericRecord] = builder.value.build()

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandler(
        out,
        new OutHandler {
          override def onDownstreamFinish(cause: Throwable): Unit =
            try reader.close()
            finally super.onDownstreamFinish(cause)

          override def onPull(): Unit = {
            val record = reader.read()
            Option(record).fold(complete(out))(push(out, _))
          }
        }
      )
    }
  override val shape: SourceShape[GenericRecord] = SourceShape.of(out)
}

private class ParquetSink(builder: AvroParquetWriter.Builder[GenericRecord])
    extends GraphStageWithMaterializedValue[SinkShape[GenericRecord], Future[Done]] {

  private val in: Inlet[GenericRecord] = Inlet("akka.parquet.sink")

  private val writer: ParquetWriter[GenericRecord] = builder.build()

  override val shape: SinkShape[GenericRecord] = SinkShape.of(in)

  override protected val initialAttributes: Attributes = super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(attr: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = new GraphStageLogic(shape) {
      setHandler(
        in,
        new InHandler {
          override def onUpstreamFinish(): Unit =
            try {
              writer.close()
              promise.complete(Success(Done))
            } catch {
              case ex: Throwable => promise.complete(Failure(ex))
            } finally super.onUpstreamFinish()

          override def onUpstreamFailure(ex: Throwable): Unit =
            try writer.close()
            finally {
              super.onUpstreamFailure(ex)
              promise.complete(Failure(ex))
            }

          override def onPush(): Unit = {
            val gr: GenericRecord = grab(in)
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
