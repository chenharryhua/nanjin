package com.github.chenharryhua.nanjin.terminals

import akka.stream.*
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.*
import cats.data.Reader
import cats.effect.kernel.Sync
import cats.Endo
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetReader, ParquetWriter}

import scala.concurrent.{Future, Promise}

final class NJParquet[F[_]] private (
  readBuilder: Reader[NJPath, ParquetReader.Builder[GenericRecord]],
  writeBuilder: Reader[NJPath, AvroParquetWriter.Builder[GenericRecord]])(implicit F: Sync[F]) {
  def updateReader(f: Endo[ParquetReader.Builder[GenericRecord]]): NJParquet[F] =
    new NJParquet(readBuilder.map(f), writeBuilder)

  def updateWriter(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): NJParquet[F] =
    new NJParquet(readBuilder, writeBuilder.map(f))

  def source(path: NJPath): Stream[F, GenericRecord] =
    for {
      rd <- Stream.bracket(F.blocking(readBuilder.run(path).build()))(r => F.blocking(r.close()))
      gr <- Stream.repeatEval(F.blocking(Option(rd.read()))).unNoneTerminate
    } yield gr

  def sink(path: NJPath): Pipe[F, GenericRecord, Nothing] = {
    def go(grs: Stream[F, GenericRecord], pw: ParquetWriter[GenericRecord]): Pull[F, Nothing, Unit] =
      grs.pull.uncons.flatMap {
        case Some((hl, tl)) => Pull.eval(F.blocking(hl.foreach(pw.write))) >> go(tl, pw)
        case None           => Pull.done
      }

    (ss: Stream[F, GenericRecord]) =>
      Stream
        .bracket(F.blocking(writeBuilder.run(path).build()))(r => F.blocking(r.close()))
        .flatMap(pw => go(ss, pw).stream)
  }

  object akka {
    def source(path: NJPath): Source[GenericRecord, Future[IOResult]] =
      Source.fromGraph(new AkkaParquetSource(readBuilder, path))

    def sink(path: NJPath): Sink[GenericRecord, Future[IOResult]] =
      Sink.fromGraph(new AkkaParquetSink(writeBuilder, path))
  }
}

object NJParquet {
  def apply[F[_]: Sync](schema: Schema, cfg: Configuration): NJParquet[F] =
    new NJParquet[F](
      readBuilder = Reader((path: NJPath) =>
        AvroParquetReader
          .builder[GenericRecord](HadoopInputFile.fromPath(path.hadoopPath, cfg))
          .withDataModel(GenericData.get())
          .withConf(cfg)),
      writeBuilder = Reader((path: NJPath) =>
        AvroParquetWriter
          .builder[GenericRecord](HadoopOutputFile.fromPath(path.hadoopPath, cfg))
          .withDataModel(GenericData.get())
          .withConf(cfg)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE))
    )
}

private class AkkaParquetSource(
  readBuilder: Reader[NJPath, ParquetReader.Builder[GenericRecord]],
  path: NJPath)
    extends GraphStageWithMaterializedValue[SourceShape[GenericRecord], Future[IOResult]] {

  private val out: Outlet[GenericRecord] = Outlet("akka.parquet.source")

  override protected val initialAttributes: Attributes =
    super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(
    inheritedAttributes: Attributes): (GraphStageLogic, Future[IOResult]) = {

    val promise: Promise[IOResult] = Promise[IOResult]()
    val logic = new GraphStageLogicWithLogging(shape) {
      override protected val logSource: Class[AkkaParquetSource] = classOf[AkkaParquetSource]
      setHandler(
        out,
        new OutHandler {
          private var count: Long = 0

          private val reader: ParquetReader[GenericRecord] = readBuilder.run(path).build()

          override def onDownstreamFinish(cause: Throwable): Unit =
            try {
              super.onDownstreamFinish(cause)
              reader.close() // close exception is a failure
              cause match {
                case _: SubscriptionWithCancelException.NonFailureCancellation =>
                  promise.success(IOResult(count))
                case ex: Throwable =>
                  promise.failure(new IOOperationIncompleteException("akka.parquet.source", count, ex))
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

private class AkkaParquetSink(
  writeBuilder: Reader[NJPath, AvroParquetWriter.Builder[GenericRecord]],
  path: NJPath)
    extends GraphStageWithMaterializedValue[SinkShape[GenericRecord], Future[IOResult]] {

  private val in: Inlet[GenericRecord] = Inlet("akka.parquet.sink")

  override val shape: SinkShape[GenericRecord] = SinkShape.of(in)

  override protected val initialAttributes: Attributes =
    super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(attr: Attributes): (GraphStageLogic, Future[IOResult]) = {
    val promise: Promise[IOResult] = Promise[IOResult]()
    val logic = new GraphStageLogicWithLogging(shape) {
      override protected val logSource: Class[AkkaParquetSink] = classOf[AkkaParquetSink]
      setHandler(
        in,
        new InHandler {
          private var count: Long = 0

          private val writer: ParquetWriter[GenericRecord] = writeBuilder.run(path).build()

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
              promise.failure(new IOOperationIncompleteException("akka.parquet.sink", count, ex))
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
