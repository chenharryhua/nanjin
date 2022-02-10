package com.github.chenharryhua.nanjin.spark.persist

import kantan.csv.{CsvConfiguration, HeaderEncoder}
import org.apache.hadoop.io.{NullWritable, Text}

import scala.tools.nsc.tasty.SafeEq

final class KantanCsv[A](enc: HeaderEncoder[A], conf: CsvConfiguration, iter: Iterator[A])
    extends Iterator[(NullWritable, Text)] {

  private def escape(str: String): String =
    if (conf.quotePolicy === CsvConfiguration.QuotePolicy.Always ||
      str.contains(conf.cellSeparator) ||
      str.contains(conf.quote)) {
      conf.quote.toString + str + conf.quote.toString
    } else {
      str
    }

  private[this] val nullWritable: NullWritable = NullWritable.get()

  override def hasNext: Boolean = iter.hasNext

  override def next(): (NullWritable, Text) =
    (nullWritable, new Text(enc.rowEncoder.encode(iter.next()).map(escape).mkString(conf.cellSeparator.toString)))
}
