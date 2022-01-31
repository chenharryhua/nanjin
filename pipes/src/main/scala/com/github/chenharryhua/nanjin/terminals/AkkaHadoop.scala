package com.github.chenharryhua.nanjin.terminals

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem}
import squants.information.Information

import java.net.URI
import scala.concurrent.Future
import scala.jdk.CollectionConverters.IteratorHasAsScala

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

  def avroSource[A](path: NJPath, schema: Schema): Source[GenericRecord, NotUsed] = {
    val dfs: DataFileStream[GenericRecord] = new DataFileStream(fsInput(path), new GenericDatumReader(schema))
    Source.fromIterator(() => dfs.iterator().asScala)
  }
}
