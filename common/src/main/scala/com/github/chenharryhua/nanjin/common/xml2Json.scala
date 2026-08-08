package com.github.chenharryhua.nanjin.common

import io.circe.Json

import scala.xml.{Elem, Node, Text}

def xml2Json(node: Node): Json =
  node match
    case elem: Elem =>
      Json.obj(elem.label -> element(elem))
    case text: Text =>
      Json.fromString(text.text)
    case oops => sys.error(oops.toString)

private def element(elem: Elem): Json = {
  val attributes =
    elem.attributes.asAttrMap.toSeq.map { case (name, value) =>
      s"@$name" -> Json.fromString(value)
    }

  val orderedNodes = elem.child.collect {
    case t: Text if t.text.trim.nonEmpty => Left(t.text.trim)
    case child: Elem                     => Right(child)
  }

  val children = orderedNodes.collect { case Right(child) => child }

  val text = orderedNodes.collect { case Left(t) => t }.mkString

  if children.isEmpty then
    if attributes.isEmpty then Json.fromString(text)
    else Json.obj(attributes :+ ("#text" -> Json.fromString(text))*)
  else
    val grouped = children.groupMapReduce(_.label)(element)(merge)

    val textField =
      Option.when(text.nonEmpty)("#text" -> Json.fromString(text)).toSeq

    val orderedChildrenField =
      Option.when(orderedNodes.exists(_.isLeft) && orderedNodes.exists(_.isRight)) {
        "#children" -> Json.arr(orderedNodes.map {
          case Left(t)      => Json.obj("#text" -> Json.fromString(t))
          case Right(child) => Json.obj(child.label -> element(child))
        }*)
      }.toSeq

    Json.obj((attributes ++ textField ++ grouped.toSeq ++ orderedChildrenField)*)
}

private def merge(a: Json, b: Json): Json =
  (a.asArray, b.asArray) match
    case (Some(xs), Some(ys)) =>
      Json.fromValues(xs ++ ys)
    case (Some(xs), None) =>
      Json.fromValues(xs :+ b)
    case (None, Some(ys)) =>
      Json.fromValues(a +: ys)
    case (None, None) =>
      Json.fromValues(Seq(a, b))
