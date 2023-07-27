package com.github.chenharryhua.nanjin.terminals

import cats.Show
import cats.kernel.Order
import com.github.chenharryhua.nanjin.common.aws.S3Path
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.string.{MatchesRegex, Uri}
import io.circe.generic.JsonCodec
import io.circe.{Decoder, Encoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}

import java.net.URI
import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

sealed abstract class PathSegment(val value: String) extends Serializable

object PathSegment extends RefinedTypeOps[NJPath.SegmentC, String] {
  def apply(ps: NJPath.SegmentC): PathSegment = new PathSegment(ps.value) {}
  def unsafe(str: String): PathSegment        = apply(unsafeFrom(str))

  implicit val encodePathSegment: Encoder[PathSegment] = Encoder.encodeString.contramap(_.value)
  implicit val decodePathSegment: Decoder[PathSegment] = Decoder.decodeString.emap(from(_).map(apply))
}

sealed abstract class PathRoot(val value: String) extends Serializable
object PathRoot extends RefinedTypeOps[NJPath.RootC, String] {
  def apply(pr: NJPath.RootC): PathRoot = new PathRoot(pr.value) {}
  def unsafe(str: String): PathRoot     = apply(unsafeFrom(str))

  implicit val encodePathRoot: Encoder[PathRoot] = Encoder.encodeString.contramap(_.value)
  implicit val decodePathRoot: Decoder[PathRoot] = Decoder.decodeString.emap(from(_).map(apply))
}

@JsonCodec
final case class NJPath private (root: PathRoot, segments: List[PathSegment]) {

  def /(seg: NJPath.SegmentC): NJPath = NJPath(root, segments.appended(PathSegment(seg)))
  def /(seg: PathSegment): NJPath     = NJPath(root, segments.appended(seg))
  def /(tn: TopicName): NJPath        = NJPath(root, segments.appended(PathSegment.unsafe(tn.value)))
  def /(uuid: UUID): NJPath           = NJPath(root, segments.appended(PathSegment.unsafe(uuid.toString)))

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
  type SegmentC = Refined[String, MatchesRegex["""^[a-zA-Z0-9_.=\-]+$"""]]
  type RootC    = Refined[String, Uri]

  def apply(root: PathRoot): NJPath         = NJPath(root, Nil)
  def apply(root: RootC): NJPath            = apply(PathRoot(root))
  def apply(hp: Path): NJPath               = apply(PathRoot.unsafe(hp.toString))
  def apply(uri: URI): NJPath               = apply(PathRoot.unsafe(uri.toASCIIString))
  def apply(s3: S3Path): NJPath             = apply(PathRoot.unsafe(s3.s3a))
  def apply(lfs: LocatedFileStatus): NJPath = apply(lfs.getPath)

  implicit final val showNJPath: Show[NJPath]         = _.pathStr
  implicit final val orderingNJPath: Ordering[NJPath] = Ordering.by(_.pathStr)
  implicit final val orderNJPath: Order[NJPath]       = Order.by(_.pathStr)
}
