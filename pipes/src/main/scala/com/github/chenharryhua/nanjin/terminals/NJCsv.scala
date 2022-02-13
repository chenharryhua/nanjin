package com.github.chenharryhua.nanjin.terminals

import akka.stream.*
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.*
import cats.effect.kernel.Sync
import cats.syntax.functor.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.{Pipe, Stream}
import io.scalaland.enumz.Enum
import kantan.csv.*
import kantan.csv.CsvConfiguration.Header
import kantan.csv.ops.{toCsvInputOps, toCsvOutputOps}
import monocle.macros.GenLens
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel

import scala.concurrent.{Future, Promise}

final class NJCsv[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  chunkSize: ChunkSize,
  compressLevel: CompressionLevel,
  csvConfiguration: CsvConfiguration
)(implicit F: Sync[F]) {
  def withChunkSize(cs: ChunkSize): NJCsv[F] =
    new NJCsv[F](configuration, blockSizeHint, cs, compressLevel, csvConfiguration)
  def withBlockSizeHint(bsh: Long): NJCsv[F] =
    new NJCsv[F](configuration, bsh, chunkSize, compressLevel, csvConfiguration)
  def withCompressionLevel(cl: CompressionLevel) =
    new NJCsv[F](configuration, blockSizeHint, chunkSize, cl, csvConfiguration)
  def withCompressionLevel(level: Int): NJCsv[F] = withCompressionLevel(Enum[CompressionLevel].withIndex(level))

  private val modify: CsvConfiguration => CsvConfiguration = GenLens[CsvConfiguration](_.header).modify {
    case Header.Implicit => Header.Explicit(List("unable to infer header", "this is a place holder"))
    case other           => other
  }

  def source[A](path: NJPath)(implicit dec: HeaderDecoder[A]): Stream[F, A] =
    for {
      is <- Stream.bracket(F.blocking(inputStream(path, configuration)))(r => F.blocking(r.close()))
      a <- Stream.fromBlockingIterator(is.asCsvReader[A](modify(csvConfiguration)).iterator, chunkSize.value).rethrow
    } yield a

  def sink[A](path: NJPath)(implicit enc: HeaderEncoder[A]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    for {
      writer <- Stream.bracket(
        F.blocking(
          outputStream(path, configuration, compressLevel, blockSizeHint).asCsvWriter[A](modify(csvConfiguration))))(
        r => F.blocking(r.close()))
      x <- ss.chunks.foreach(c => F.blocking(c.map(writer.write)).void)
    } yield x
  }

  object akka {
    def source[A](path: NJPath)(implicit dec: HeaderDecoder[A]): Source[A, Future[IOResult]] =
      Source.fromGraph(new AkkaCsvSource[A](path, modify(csvConfiguration), configuration))

    def sink[A](path: NJPath)(implicit enc: HeaderEncoder[A]): Sink[A, Future[IOResult]] =
      Sink.fromGraph(new AkkaCsvSink[A](path, modify(csvConfiguration), configuration, blockSizeHint, compressLevel))
  }
}
object NJCsv {
  def apply[F[_]: Sync](csvConfiguration: CsvConfiguration, cfg: Configuration) =
    new NJCsv[F](cfg, BlockSizeHint, ChunkSize(1000), CompressionLevel.DEFAULT_COMPRESSION, csvConfiguration)
}

private class AkkaCsvSource[A](path: NJPath, csvConfiguration: CsvConfiguration, configuration: Configuration)(implicit
  dec: HeaderDecoder[A])
    extends GraphStageWithMaterializedValue[SourceShape[A], Future[IOResult]] {
  private val out: Outlet[A] = Outlet("akka.csv.source")

  override val shape: SourceShape[A] = SourceShape.of(out)

  override protected val initialAttributes: Attributes = super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[IOResult]) = {
    val promise: Promise[IOResult] = Promise[IOResult]()
    val logic = new GraphStageLogicWithLogging(shape) {
      override protected val logSource: Class[AkkaParquetSource] = classOf[AkkaParquetSource]
      setHandler(
        out,
        new OutHandler {
          private var count: Long = 0

          private val reader: CsvReader[ReadResult[A]] =
            inputStream(path, configuration).asCsvReader[A](csvConfiguration)

          override def onDownstreamFinish(cause: Throwable): Unit =
            try {
              super.onDownstreamFinish(cause)
              reader.close() // close exception is a failure
              cause match {
                case _: SubscriptionWithCancelException.NonFailureCancellation =>
                  promise.success(IOResult(count))
                case ex: Throwable =>
                  promise.failure(new IOOperationIncompleteException("csv.source", count, ex))
              }
            } catch {
              case ex: Throwable => promise.failure(ex)
            }

          override def onPull(): Unit =
            if (reader.hasNext) {
              reader.next() match {
                case Left(ex) => throw ex
                case Right(value) =>
                  count += 1
                  push(out, value)
              }
            } else {
              completeStage()
            }
        }
      )
    }
    (logic, promise.future)
  }
}

private class AkkaCsvSink[A](
  path: NJPath,
  csvConfiguration: CsvConfiguration,
  configuration: Configuration,
  blockSizeHint: Long,
  compressLevel: CompressionLevel)(implicit enc: HeaderEncoder[A])
    extends GraphStageWithMaterializedValue[SinkShape[A], Future[IOResult]] {

  private val in: Inlet[A] = Inlet("akka.csv.sink")

  override val shape: SinkShape[A] = SinkShape.of(in)

  override protected val initialAttributes: Attributes = super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(attr: Attributes): (GraphStageLogic, Future[IOResult]) = {
    val promise: Promise[IOResult] = Promise[IOResult]()
    val logic = new GraphStageLogicWithLogging(shape) {
      override protected val logSource: Class[AkkaParquetSink] = classOf[AkkaParquetSink]
      setHandler(
        in,
        new InHandler {
          private var count: Long = 0

          private val writer: CsvWriter[A] =
            outputStream(path, configuration, compressLevel, blockSizeHint).asCsvWriter[A](csvConfiguration)

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
              promise.failure(new IOOperationIncompleteException("csv.sink", count, ex))
            } catch {
              case ex: Throwable => promise.failure(ex)
            }
          override def onPush(): Unit = {
            val gr: A = grab(in)
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