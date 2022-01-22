package com.github.chenharryhua.nanjin.terminals

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.boolean.{And, Not}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.collection.{Contains, NonEmpty}
import eu.timepit.refined.string.{Trimmed, Uri}
import org.apache.hadoop.fs.Path

import java.net.URI
import java.time.{LocalDate, ZonedDateTime}

final case class NJPath private (root: NJPath.Root, segments: List[NJPath.Segment]) {

  def /(seg: NJPath.Segment): NJPath = NJPath(root, segments.appended(seg))

  def /(ld: LocalDate): NJPath = {
    val year  = NJPath.Segment.unsafeFrom(f"Year=${ld.getYear}%4d")
    val month = NJPath.Segment.unsafeFrom(f"Month=${ld.getMonthValue}%02d")
    val day   = NJPath.Segment.unsafeFrom(f"Day=${ld.getDayOfMonth}%02d")
    NJPath(root, segments ::: List(year, month, day))
  }

  def /(zdt: ZonedDateTime): NJPath = {
    val year  = NJPath.Segment.unsafeFrom(f"Year=${zdt.getYear}%4d")
    val month = NJPath.Segment.unsafeFrom(f"Month=${zdt.getMonthValue}%02d")
    val day   = NJPath.Segment.unsafeFrom(f"Day=${zdt.getDayOfMonth}%02d")
    val hour  = NJPath.Segment.unsafeFrom(f"Hour=${zdt.getHour}%02d")
    NJPath(root, segments ::: List(year, month, day, hour))
  }

  def uri: URI = {
    val r: String = if (root.value.endsWith("/")) root.value.dropRight(1) else root.value
    val p: String = s"$r/${segments.map(_.value).mkString("/")}"
    new URI(if (p.endsWith("/")) p.dropRight(1) else p)
  }

  def pathStr: String = uri.toASCIIString

  def hadoopPath: Path = new Path(uri)
}
object NJPath {
  type Segment = Refined[String, And[And[NonEmpty, Trimmed], Not[Contains['/']]]]
  object Segment extends RefinedTypeOps[Segment, String] with CatsRefinedTypeOpsSyntax

  type Root = Refined[String, Uri]
  object Root extends RefinedTypeOps[Root, String] with CatsRefinedTypeOpsSyntax

  def apply(root: Refined[String, Uri]): NJPath = NJPath(root, Nil)
}
