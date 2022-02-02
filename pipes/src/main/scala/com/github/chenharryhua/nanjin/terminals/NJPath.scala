package com.github.chenharryhua.nanjin.terminals

import cats.Show
import com.github.chenharryhua.nanjin.common.{PathRoot, PathSegment}
import eu.timepit.refined.api.Refined
import org.apache.hadoop.fs.Path

import java.net.URI
import java.time.{LocalDate, ZonedDateTime}

final case class NJPath private (root: PathRoot, segments: List[PathSegment]) {

  def /(seg: PathSegment): NJPath = NJPath(root, segments.appended(seg))

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

  lazy val uri: URI = new URI(root.value + segments.map(g => s"/${g.value}").mkString).normalize()

  lazy val pathStr: String = uri.toASCIIString

  lazy val hadoopPath: Path = new Path(uri)

  override lazy val toString: String = pathStr
}
object NJPath {

  def apply(root: PathRoot): NJPath = NJPath(root, Nil)
  def apply(hp: Path): NJPath       = NJPath(PathRoot.unsafeFrom(hp.toString))

  implicit val showNJPath: Show[NJPath] = _.pathStr

}
