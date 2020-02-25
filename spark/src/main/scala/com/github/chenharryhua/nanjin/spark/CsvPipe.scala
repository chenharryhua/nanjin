package com.github.chenharryhua.nanjin.spark

import java.io.InputStream

import cats.effect.{Blocker, ContextShift, Sync}
import fs2.Stream
import kantan.csv.ops._
import kantan.csv.{rfc, HeaderDecoder}
import org.apache.commons.io.input.ReaderInputStream
import org.apache.spark.sql.SparkSession

final class CsvPipe {

  def csvSource[F[_]: Sync: ContextShift, A: HeaderDecoder](in: InputStream, blocker: Blocker) = {
    Stream.fromBlockingIterator[F](blocker, in.asCsvReader[A](rfc).toIterator).rethrow
  }
}
