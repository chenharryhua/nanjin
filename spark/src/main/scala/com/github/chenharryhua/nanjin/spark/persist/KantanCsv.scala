package com.github.chenharryhua.nanjin.spark.persist

import kantan.csv.CsvConfiguration.Header
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import org.apache.hadoop.io.{NullWritable, Text}

final class KantanCsv[A](enc: HeaderEncoder[A], conf: CsvConfiguration, iter: Iterator[A])
    extends Iterator[(NullWritable, Text)] {

  private def escape(str: String): String =
    if (conf.quotePolicy == CsvConfiguration.QuotePolicy.Always ||
      str.contains(conf.cellSeparator) ||
      str.contains(conf.quote)) {
      conf.quote.toString + str + conf.quote.toString
    } else {
      str
    }

  private[this] val nullWritable: NullWritable = NullWritable.get()

  private val header: Option[(NullWritable, Text)] = {
    val h = conf.header match {
      case Header.None             => None
      case Header.Implicit         => enc.header
      case Header.Explicit(header) => Some(header)
    }
    h.map(_.map(escape).mkString(conf.cellSeparator.toString)).map(x => (nullWritable, new Text(x)))
  }

  private[this] var isFirstTimeAccess: Boolean = true

  private[this] def nextText(): Text =
    new Text(enc.rowEncoder.encode(iter.next()).map(escape).mkString(conf.cellSeparator.toString))

  override def hasNext: Boolean = iter.hasNext

  override def next(): (NullWritable, Text) =
    if (isFirstTimeAccess) {
      isFirstTimeAccess = false
      header match {
        case Some(value) => value
        case None        => (nullWritable, nextText())
      }
    } else (nullWritable, nextText())
}
