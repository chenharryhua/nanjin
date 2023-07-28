package com.github.chenharryhua.nanjin.terminals

import cats.Show
import cats.kernel.Order
import com.github.chenharryhua.nanjin.common.aws.S3Path
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.string.Uri
import io.circe.generic.JsonCodec
import io.circe.{Decoder, Encoder}
import org.apache.hadoop.fs.{LocatedFileStatus, Path}

import java.net.URI
import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

final class PathRoot private (val value: String) extends Serializable
object PathRoot extends RefinedTypeOps[NJPath.RootC, String] {
  def apply(pr: NJPath.RootC): PathRoot = new PathRoot(pr.value)
  def unsafe(str: String): PathRoot     = apply(unsafeFrom(str))

  implicit val encodePathRoot: Encoder[PathRoot] = Encoder.encodeString.contramap(_.value)
  implicit val decodePathRoot: Decoder[PathRoot] = Decoder.decodeString.emap(from(_).map(apply))
}

@JsonCodec
final case class NJPath private (root: PathRoot, segments: List[String]) {

  def /(seg: String): NJPath   = copy(segments = segments.appended(seg))
  def /(tn: TopicName): NJPath = copy(segments = segments.appended(tn.value))
  def /(uuid: UUID): NJPath    = copy(segments = segments.appended(uuid.toString))

  def /(num: Long): NJPath = copy(segments = segments.appended(num.toString))
  def /(num: Int): NJPath  = copy(segments = segments.appended(num.toString))

  // Year=2020/Month=01/Day=05
  def /(ld: LocalDate): NJPath = {
    val year  = f"Year=${ld.getYear}%4d"
    val month = f"Month=${ld.getMonthValue}%02d"
    val day   = f"Day=${ld.getDayOfMonth}%02d"
    copy(segments = segments ::: List(year, month, day))
  }

  // Year=2020/Month=01/Day=05/Hour=23
  def /(ldt: LocalDateTime): NJPath = {
    val year  = f"Year=${ldt.getYear}%4d"
    val month = f"Month=${ldt.getMonthValue}%02d"
    val day   = f"Day=${ldt.getDayOfMonth}%02d"
    val hour  = f"Hour=${ldt.getHour}%02d"
    copy(segments = segments ::: List(year, month, day, hour))
  }

  lazy val uri: URI = new URI(root.value + segments.map(g => s"/$g").mkString).normalize()

  lazy val pathStr: String = uri.toASCIIString

  lazy val hadoopPath: Path = new Path(uri)

  override lazy val toString: String = pathStr
}

object NJPath {
  type RootC = Refined[String, Uri]

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
