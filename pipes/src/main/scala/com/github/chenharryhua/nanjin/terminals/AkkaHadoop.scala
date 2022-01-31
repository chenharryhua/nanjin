package com.github.chenharryhua.nanjin.terminals

import akka.{Done, NotUsed}
import akka.stream.{ActorAttributes, Attributes, IOResult, Inlet, SinkShape}
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.util.ByteString
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem}
import squants.information.Information

import java.io.OutputStream
import java.net.URI
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success}

final class AkkaHadoop(config: Configuration) {
  private def fileSystem(uri: URI): FileSystem           = FileSystem.get(uri, config)
  private def fsOutput(path: NJPath): FSDataOutputStream = fileSystem(path.uri).create(path.hadoopPath)
  private def fsInput(path: NJPath): FSDataInputStream   = fileSystem(path.uri).open(path.hadoopPath)

  def byteSource(path: NJPath, byteBuffer: Information): Source[ByteString, Future[IOResult]] =
    StreamConverters.fromInputStream(() => fsInput(path), byteBuffer.toBytes.toInt)
  def byteSource(path: NJPath): Source[ByteString, Future[IOResult]] =
    StreamConverters.fromInputStream(() => fsInput(path))

  def byteSink(path: NJPath): Sink[ByteString, Future[IOResult]] =
    StreamConverters.fromOutputStream(() => fsOutput(path))

  def avroSource(path: NJPath, schema: Schema): Source[GenericRecord, NotUsed] = {
    val dfs: DataFileStream[GenericRecord] = new DataFileStream(fsInput(path), new GenericDatumReader(schema))
    Source.fromIterator(() => dfs.iterator().asScala)
  }
  def avroSink(path: NJPath, schema: Schema, codecFactory: CodecFactory): Sink[GenericRecord, Future[Done]] =
    Sink.fromGraph(new AvroSink(fsOutput(path), schema, codecFactory))
}

private class AvroSink(os: OutputStream, schema: Schema, codecFactory: CodecFactory)
    extends GraphStageWithMaterializedValue[SinkShape[GenericRecord], Future[Done]] {

  private val in: Inlet[GenericRecord] = Inlet("akka.avro.sink")

  private val writer: DataFileWriter[GenericRecord] =
    new DataFileWriter(new GenericDatumWriter[GenericRecord](schema, GenericData.get()))
      .setCodec(codecFactory)
      .create(schema, os)

  override val shape: SinkShape[GenericRecord] = SinkShape.of(in)

  override protected val initialAttributes: Attributes = super.initialAttributes.and(ActorAttributes.IODispatcher)

  override def createLogicAndMaterializedValue(attr: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = new GraphStageLogic(shape) {
      setHandler(
        in,
        new InHandler {
          override def onUpstreamFinish(): Unit = {
            super.onUpstreamFinish()
            try {
              writer.close()
              os.close()
              promise.complete(Success(Done))
            } catch {
              case ex: Throwable => promise.complete(Failure(ex))
            }
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            super.onUpstreamFailure(ex)
            try {
              writer.close()
              os.close()
            } finally promise.complete(Failure(ex))
          }

          override def onPush(): Unit = {
            val gr: GenericRecord = grab(in)
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
