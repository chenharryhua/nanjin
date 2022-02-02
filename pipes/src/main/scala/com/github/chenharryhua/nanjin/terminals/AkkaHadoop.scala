package com.github.chenharryhua.nanjin.terminals

import akka.stream.{
  ActorAttributes,
  Attributes,
  IOOperationIncompleteException,
  IOResult,
  Inlet,
  Outlet,
  SinkShape,
  SourceShape,
  SubscriptionWithCancelException
}
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.stream.stage.{
  GraphStageLogic,
  GraphStageLogicWithLogging,
  GraphStageWithMaterializedValue,
  InHandler,
  OutHandler
}
import akka.util.ByteString
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem}
import squants.information.Information

import java.io.{InputStream, OutputStream}
import java.net.URI
import scala.concurrent.{Future, Promise}

final class AkkaHadoop private (config: Configuration) {
  private def fileSystem(uri: URI): FileSystem           = FileSystem.get(uri, config)
  private def fsOutput(path: NJPath): FSDataOutputStream = fileSystem(path.uri).create(path.hadoopPath)
  private def fsInput(path: NJPath): FSDataInputStream   = fileSystem(path.uri).open(path.hadoopPath)

  def byteSource(path: NJPath, byteBuffer: Information): Source[ByteString, Future[IOResult]] =
    StreamConverters.fromInputStream(() => fsInput(path), byteBuffer.toBytes.toInt)
  def byteSource(path: NJPath): Source[ByteString, Future[IOResult]] =
    StreamConverters.fromInputStream(() => fsInput(path))

  def byteSink(path: NJPath): Sink[ByteString, Future[IOResult]] =
    StreamConverters.fromOutputStream(() => fsOutput(path))

  def avroSource(path: NJPath, schema: Schema): Source[GenericRecord, Future[IOResult]] =
    Source.fromGraph(new AvroSource(fsInput(path), schema))
  def avroSink(path: NJPath, schema: Schema, codecFactory: CodecFactory): Sink[GenericRecord, Future[IOResult]] =
    Sink.fromGraph(new AvroSink(fsOutput(path), schema, codecFactory))
}
object AkkaHadoop {
  def apply(cfg: Configuration): AkkaHadoop = new AkkaHadoop(cfg)
}

private class AvroSource(is: InputStream, schema: Schema)
    extends GraphStageWithMaterializedValue[SourceShape[GenericRecord], Future[IOResult]] {
  private val out: Outlet[GenericRecord] = Outlet("akka.avro.source")

  override protected val initialAttributes: Attributes = super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(attr: Attributes): (GraphStageLogic, Future[IOResult]) = {
    val promise: Promise[IOResult] = Promise[IOResult]()
    val logic = new GraphStageLogicWithLogging(shape) {
      override protected val logSource: Class[AvroSource] = classOf[AvroSource]
      setHandler(
        out,
        new OutHandler {
          private var count: Long = 0

          private val reader: DataFileStream[GenericRecord] =
            new DataFileStream(is, new GenericDatumReader[GenericRecord](schema))

          override def onDownstreamFinish(cause: Throwable): Unit =
            try {
              super.onDownstreamFinish(cause)
              reader.close()
              is.close()
              cause match {
                case _: SubscriptionWithCancelException.NonFailureCancellation =>
                  promise.success(IOResult(count))
                case ex: Throwable =>
                  promise.failure(new IOOperationIncompleteException("avro.source", count, ex))
              }
            } catch {
              case ex: Throwable => promise.failure(ex)
            }

          override def onPull(): Unit =
            if (reader.hasNext) {
              count += 1
              push(out, reader.next())
            } else complete(out)
        }
      )
    }
    (logic, promise.future)
  }

  override val shape: SourceShape[GenericRecord] = SourceShape.of(out)
}

private class AvroSink(os: OutputStream, schema: Schema, codecFactory: CodecFactory)
    extends GraphStageWithMaterializedValue[SinkShape[GenericRecord], Future[IOResult]] {

  private val in: Inlet[GenericRecord] = Inlet("akka.avro.sink")

  override val shape: SinkShape[GenericRecord] = SinkShape.of(in)

  override protected val initialAttributes: Attributes = super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(attr: Attributes): (GraphStageLogic, Future[IOResult]) = {
    val promise: Promise[IOResult] = Promise[IOResult]()
    val logic = new GraphStageLogicWithLogging(shape) {
      override protected val logSource: Class[AvroSink] = classOf[AvroSink]

      setHandler(
        in,
        new InHandler {
          private var count: Long = 0

          private val writer: DataFileWriter[GenericRecord] =
            new DataFileWriter(new GenericDatumWriter[GenericRecord](schema, GenericData.get()))
              .setCodec(codecFactory)
              .create(schema, os)

          override def onUpstreamFinish(): Unit =
            try {
              super.onUpstreamFinish()
              writer.close()
              os.close()
              promise.success(IOResult(count))
            } catch {
              case ex: Throwable => promise.failure(ex)
            }

          override def onUpstreamFailure(ex: Throwable): Unit =
            try {
              super.onUpstreamFailure(ex)
              writer.close()
              os.close()
              promise.failure(new IOOperationIncompleteException("avro.sink", count, ex))
            } catch {
              case ex: Throwable => promise.failure(ex)
            }

          override def onPush(): Unit = {
            val gr: GenericRecord = grab(in)
            count += 1
            writer.append(gr)
            pull(in)
          }
        }
      )
      override def preStart(): Unit = pull(in)
    }
    (logic, promise.future)
  }
}
