package com.github.chenharryhua.nanjin.terminals

import cats.Show
import cats.kernel.Order
import com.github.chenharryhua.nanjin.common.aws.S3Path
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.terminals
import eu.timepit.refined.api.RefinedTypeOps
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import io.circe.{Decoder, Encoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}

import java.net.URI
import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

final class PathSegment private (val value: String) extends Serializable

object PathSegment extends RefinedTypeOps[PathSegmentC, String] with CatsRefinedTypeOpsSyntax {
  def apply(ps: PathSegmentC): PathSegment = new PathSegment(ps.value)
  def unsafe(str: String): PathSegment     = apply(unsafeFrom(str))
}

final class PathRoot private (val value: String) extends Serializable
object PathRoot extends RefinedTypeOps[PathRootC, String] with CatsRefinedTypeOpsSyntax {
  def apply(pr: PathRootC): PathRoot = new PathRoot(pr.value)
  def unsafe(str: String): PathRoot  = apply(unsafeFrom(str))
}

final case class NJPath private (root: PathRoot, segments: List[PathSegment]) {

  def /(seg: PathSegmentC): NJPath = NJPath(root, segments.appended(terminals.PathSegment(seg)))
  def /(seg: PathSegment): NJPath  = NJPath(root, segments.appended(seg))
  def /(tn: TopicName): NJPath     = NJPath(root, segments.appended(PathSegment.unsafe(tn.value)))
  def /(uuid: UUID): NJPath        = NJPath(root, segments.appended(PathSegment.unsafe(uuid.toString)))

  def /(num: Long): NJPath = NJPath(root, segments.appended(PathSegment.unsafe(num.toString)))

  def /(num: Int): NJPath = NJPath(root, segments.appended(PathSegment.unsafe(num.toString)))

  // Year=2020/Month=01/Day=05
  def /(ld: LocalDate): NJPath = {
    val year  = PathSegment.unsafe(f"Year=${ld.getYear}%4d")
    val month = PathSegment.unsafe(f"Month=${ld.getMonthValue}%02d")
    val day   = PathSegment.unsafe(f"Day=${ld.getDayOfMonth}%02d")
    NJPath(root, segments ::: List(year, month, day))
  }

  // Year=2020/Month=01/Day=05/Hour=23
  def /(ldt: LocalDateTime): NJPath = {
    val year  = PathSegment.unsafe(f"Year=${ldt.getYear}%4d")
    val month = PathSegment.unsafe(f"Month=${ldt.getMonthValue}%02d")
    val day   = PathSegment.unsafe(f"Day=${ldt.getDayOfMonth}%02d")
    val hour  = PathSegment.unsafe(f"Hour=${ldt.getHour}%02d")
    NJPath(root, segments ::: List(year, month, day, hour))
  }

  lazy val uri: URI = new URI(root.value + segments.map(g => s"/${g.value}").mkString).normalize()

  lazy val pathStr: String = uri.toASCIIString

  lazy val hadoopPath: Path = new Path(uri)

  def hadoopOutputFile(cfg: Configuration): HadoopOutputFile = HadoopOutputFile.fromPath(hadoopPath, cfg)
  def hadoopInputFile(cfg: Configuration): HadoopInputFile   = HadoopInputFile.fromPath(hadoopPath, cfg)

  override lazy val toString: String = pathStr
}

object NJPath {
  implicit val encoderNJPath: Encoder[NJPath] = Encoder.encodeString.contramap(_.pathStr)
  implicit val decoderNJPath: Decoder[NJPath] = Decoder.decodeURI.map(apply)

  def apply(root: PathRoot): NJPath         = NJPath(root, Nil)
  def apply(root: PathRootC): NJPath        = apply(PathRoot(root))
  def apply(hp: Path): NJPath               = apply(PathRoot.unsafe(hp.toString))
  def apply(uri: URI): NJPath               = apply(PathRoot.unsafe(uri.toASCIIString))
  def apply(s3: S3Path): NJPath             = apply(PathRoot.unsafe(s3.s3a))
  def apply(lfs: LocatedFileStatus): NJPath = apply(lfs.getPath)

  implicit final val showNJPath: Show[NJPath] = _.pathStr

  implicit final val orderingNJPath: Ordering[NJPath] = Ordering.by(_.pathStr)
  implicit final val orderNJPath: Order[NJPath]       = Order.by(_.pathStr)
}
