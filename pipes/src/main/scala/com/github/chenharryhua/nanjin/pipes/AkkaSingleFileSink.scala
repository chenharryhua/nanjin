package com.github.chenharryhua.nanjin.pipes

import java.net.URI

import akka.Done
import akka.stream.scaladsl.Sink
import cats.effect.{Async, Blocker, ContextShift}
import com.sksamuel.avro4s.{
  AvroOutputStream,
  AvroOutputStreamBuilder,
  DefaultFieldMapper,
  SchemaFor,
  Encoder => AvroEncoder
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

final class AkkaSingleFileSink[F[_]: Async: ContextShift](hadoopConfig: Configuration) {

  def jackson[A: SchemaFor: AvroEncoder](pathStr: String) = {
    val fs  = FileSystem.get(new URI(pathStr), hadoopConfig)
    val os  = fs.create(new Path(pathStr))
    val aos = AvroOutputStream.json[A].to(os).build(SchemaFor[A].schema(DefaultFieldMapper))
    Sink.foreach[A](aos.write)
  }
}
