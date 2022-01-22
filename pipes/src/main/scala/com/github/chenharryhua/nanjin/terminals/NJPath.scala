package com.github.chenharryhua.nanjin.terminals

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.boolean.{And, Not}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.collection.{Contains, NonEmpty}
import eu.timepit.refined.string.{Trimmed, Uri}
import org.apache.hadoop.fs.Path

import java.net.URI

final case class NJPath private (root: NJPath.Root, segments: List[NJPath.Segment]) {

  def /(seg: NJPath.Segment): NJPath = NJPath(root, segments.appended(seg))

  def uri: URI = new URI(
    if (root.value.endsWith("/")) s"${root.value}${segments.map(_.value).mkString("/")}"
    else s"${root.value}/${segments.map(_.value).mkString("/")}")

  def pathStr: String = uri.getPath

  def hadoopPath: Path = new Path(pathStr)
}
object NJPath {
  type Segment = Refined[String, And[And[NonEmpty, Trimmed], Not[Contains['/']]]]
  object Segment extends RefinedTypeOps[Segment, String] with CatsRefinedTypeOpsSyntax

  type Root = Refined[String, Uri]
  object Root extends RefinedTypeOps[Root, String] with CatsRefinedTypeOpsSyntax

  def apply(root: Refined[String, Uri]): NJPath = NJPath(root, Nil)
}
