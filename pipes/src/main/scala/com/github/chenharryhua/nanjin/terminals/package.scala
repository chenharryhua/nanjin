package com.github.chenharryhua.nanjin

import cats.implicits.catsSyntaxEq
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.numeric.Interval.Closed
import fs2.Chunk
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import kantan.csv.CsvConfiguration.Header
import kantan.csv.engine.WriterEngine
import org.apache.hadoop.fs.Path

import java.io.StringWriter

package object terminals {

  type NJCompressionLevel = Int Refined Closed[1, 9]
  object NJCompressionLevel extends RefinedTypeOps[NJCompressionLevel, Int] with CatsRefinedTypeOpsSyntax

  def csvRow(csvConfiguration: CsvConfiguration)(row: Seq[String]): String = {
    val sw = new StringWriter()
    WriterEngine.internalCsvWriterEngine.writerFor(sw, csvConfiguration).write(row).close()
    sw.toString
  }

  def csvHeader(csvConfiguration: CsvConfiguration): Chunk[String] =
    csvConfiguration.header match {
      case Header.None             => Chunk.empty
      case Header.Implicit         => Chunk.singleton("no header was explicitly provided\r\n") // csv use CRLF
      case Header.Explicit(header) => Chunk.singleton(csvRow(csvConfiguration)(header))
    }

  def toHadoopPath(url: Url): Path =
    if (url.schemeOption.exists(_ === "s3")) {
      new Path(url.withScheme("s3a").normalize(removeEmptyPathParts = true).toJavaURI)
    } else
      new Path(url.normalize(removeEmptyPathParts = true).toJavaURI)
}
