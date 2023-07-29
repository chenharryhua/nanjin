package com.github.chenharryhua.nanjin.terminals

import cats.Show
import cats.data.NonEmptyList
import cats.kernel.Order
import com.github.chenharryhua.nanjin.common.aws.S3Path
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.time.{year_month_day, year_month_day_hour}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.refineV
import eu.timepit.refined.string.Uri
import io.circe.{Decoder, Encoder}
import org.apache.hadoop.fs.{LocatedFileStatus, Path}

import java.net.URI
import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

final class NJPath private (segments: NonEmptyList[String]) extends Serializable {

  def /(seg: String): NJPath   = new NJPath(segments.append(seg))
  def /(tn: TopicName): NJPath = new NJPath(segments.append(tn.value))
  def /(uuid: UUID): NJPath    = new NJPath(segments.append(uuid.toString))

  def /(num: Long): NJPath = new NJPath(segments.append(num.toString))
  def /(num: Int): NJPath  = new NJPath(segments.append(num.toString))

  // Year=2020/Month=01/Day=05
  def /(ld: LocalDate): NJPath =
    new NJPath(segments.append(year_month_day(ld)))

  // Year=2020/Month=01/Day=05/Hour=23
  def /(ldt: LocalDateTime): NJPath =
    new NJPath(segments.append(year_month_day_hour(ldt)))

  lazy val uri: URI         = new URI(segments.map(_.trim).filter(_.nonEmpty).mkString("/")).normalize()
  lazy val pathStr: String  = uri.toASCIIString
  lazy val hadoopPath: Path = new Path(uri)

  override lazy val toString: String = pathStr
}

object NJPath {
  def apply(root: Refined[String, Uri]): NJPath = new NJPath(NonEmptyList.one(root.value))

  def apply(hp: Path): NJPath   = new NJPath(NonEmptyList.one(hp.toString))
  def apply(s3: S3Path): NJPath = new NJPath(NonEmptyList.one(s3.s3a))

  def apply(lfs: LocatedFileStatus): NJPath = apply(lfs.getPath)
  def apply(uri: URI): NJPath               = apply(new Path(uri))

  implicit final val showNJPath: Show[NJPath]         = p => s"NJPath(uri=${p.pathStr})"
  implicit final val orderingNJPath: Ordering[NJPath] = Ordering.by(_.pathStr)
  implicit final val orderNJPath: Order[NJPath]       = Order.by(_.pathStr)

  implicit final val encodeNJPath: Encoder[NJPath] = Encoder.encodeString.contramap(_.pathStr)
  implicit final val decodeNJPath: Decoder[NJPath] = Decoder.decodeString.emap(refineV[Uri](_)).map(apply)
}
