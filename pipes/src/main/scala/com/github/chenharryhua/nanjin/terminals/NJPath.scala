package com.github.chenharryhua.nanjin.terminals

import cats.{Eq, Show}
import com.github.chenharryhua.nanjin.common.aws.S3Path
import com.github.chenharryhua.nanjin.common.{PathRoot, PathSegment}
import eu.timepit.refined.api.Refined
import io.circe.generic.JsonCodec
import io.circe.refined.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import cats.syntax.eq.*
import java.net.URI
import java.time.{LocalDate, ZonedDateTime}

@JsonCodec
final case class NJPath private (root: PathRoot, segments: List[PathSegment]) {

  def /(seg: PathSegment): NJPath      = NJPath(root, segments.appended(seg))
  def /(ss: List[PathSegment]): NJPath = NJPath(root, segments ::: ss)

  // Year=2020/Month=01/Day=05
  def /(ld: LocalDate): NJPath = {
    val year  = PathSegment.unsafeFrom(f"Year=${ld.getYear}%4d")
    val month = PathSegment.unsafeFrom(f"Month=${ld.getMonthValue}%02d")
    val day   = PathSegment.unsafeFrom(f"Day=${ld.getDayOfMonth}%02d")
    NJPath(root, segments ::: List(year, month, day))
  }

  // Year=2020/Month=01/Day=05/Hour=23
  def /(zdt: ZonedDateTime): NJPath = {
    val year  = PathSegment.unsafeFrom(f"Year=${zdt.getYear}%4d")
    val month = PathSegment.unsafeFrom(f"Month=${zdt.getMonthValue}%02d")
    val day   = PathSegment.unsafeFrom(f"Day=${zdt.getDayOfMonth}%02d")
    val hour  = PathSegment.unsafeFrom(f"Hour=${zdt.getHour}%02d")
    NJPath(root, segments ::: List(year, month, day, hour))
  }

  def /(num: Long): NJPath = NJPath(root, segments.appended(PathSegment.unsafeFrom(num.toString)))

  lazy val uri: URI = new URI(root.value + segments.map(g => s"/${g.value}").mkString).normalize()

  lazy val pathStr: String = uri.toASCIIString

  lazy val hadoopPath: Path = new Path(uri)

  def hadoopOutputFile(cfg: Configuration): HadoopOutputFile = HadoopOutputFile.fromPath(hadoopPath, cfg)
  def hadoopInputFile(cfg: Configuration): HadoopInputFile   = HadoopInputFile.fromPath(hadoopPath, cfg)

  override lazy val toString: String = pathStr
}
object NJPath {

  def apply(root: PathRoot): NJPath         = NJPath(root, Nil)
  def apply(hp: Path): NJPath               = apply(PathRoot.unsafeFrom(hp.toString))
  def apply(uri: URI): NJPath               = apply(PathRoot.unsafeFrom(uri.toASCIIString))
  def apply(s3: S3Path): NJPath             = apply(PathRoot.unsafeFrom(s3.s3a))
  def apply(lfs: LocatedFileStatus): NJPath = apply(lfs.getPath)

  implicit final val showNJPath: Show[NJPath] = _.pathStr
  implicit final val eqNJPath: Eq[NJPath]     = (x: NJPath, y: NJPath) => x.pathStr === y.pathStr

}
