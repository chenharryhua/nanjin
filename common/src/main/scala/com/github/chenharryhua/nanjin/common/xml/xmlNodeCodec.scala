package com.github.chenharryhua.nanjin.common.xml

import cats.syntax.eq.catsSyntaxEq
import io.circe.{Codec, Decoder, DecodingFailure, Encoder, Json}

import scala.xml.{Elem, MetaData, Node, Null, Text, TopScope, UnprefixedAttribute}

given Codec[Node] = Codec.from(decodeNode, Encoder.instance(encodeNode))

private def encodeNode(node: Node): Json =
  node match
    case elem: Elem => Json.obj(elem.label -> element(elem))
    case text: Text => Json.fromString(text.text)
    case oops       => sys.error(s"unknown xml node: $oops")

private def decodeNode: Decoder[Node] =
  Decoder.instance { c =>
    c.value.asString match
      case Some(text) => Right(Text(text))
      case None       =>
        c.value.asObject match
          case Some(obj) if obj.size === 1 =>
            val (label, payload) = obj.toIterable.head
            decodeElement(label, payload).flatMap(validateRoundTrip(c.value, c.history))
          case _ =>
            Left(DecodingFailure("expects a JSON string or a single-field root object", c.history))
  }

private def validateRoundTrip(original: Json, history: List[io.circe.CursorOp])(
  node: Node): Decoder.Result[Node] =
  Either.cond(
    encodeNode(node) === original,
    node,
    DecodingFailure("only decodes canonical JSON produced by this codec", history)
  )

private def decodeElement(label: String, payload: Json): Decoder.Result[Elem] =
  payload.asString match
    case Some(text) =>
      Right(Elem(null, label, Null, TopScope, true, Text(text)))
    case None =>
      payload.asObject match
        case Some(obj) =>
          val attrs = obj.toVector.collect { case (k, v) if k.startsWith("@") => (k.drop(1), v) }
          val meta = attrs.foldRight(Null: MetaData) { case ((k, v), acc) =>
            val value = v.asString.getOrElse(v.noSpaces)
            UnprefixedAttribute(k, value, acc)
          }

          val text =
            obj("#text").flatMap(_.asString).getOrElse("")

          val orderedChildren =
            obj("#children").flatMap(_.asArray) match
              case Some(arr) =>
                arr.toList
                  .foldRight(Right(List.empty[Node]): Decoder.Result[List[Node]]) { (js, acc) =>
                    for
                      tail <- acc
                      head <- decodeOrderedNode(js)
                    yield head :: tail
                  }
              case None =>
                val groupedPairs = obj.toVector.filterNot { case (k, _) =>
                  k.startsWith("@") || k === "#text" || k === "#children"
                }
                groupedPairs
                  .foldRight(Right(List.empty[Node]): Decoder.Result[List[Node]]) { case ((k, v), acc) =>
                    for
                      tail <- acc
                      nodes <- decodeGroupedChildren(k, v)
                    yield nodes ::: tail
                  }

          orderedChildren.map { children =>
            val nodes =
              if children.nonEmpty then children
              else List(Text(text))

            Elem(null, label, meta, TopScope, true, nodes*)
          }
        case None =>
          Left(DecodingFailure(s"Invalid element payload for <$label>", Nil))

private def decodeOrderedNode(js: Json): Decoder.Result[Node] =
  js.asObject match
    case Some(obj) if obj.size === 1 && obj.contains("#text") =>
      obj("#text").flatMap(_.asString) match
        case Some(text) => Right(Text(text))
        case None       => Left(DecodingFailure("#children text entry must be a string", Nil))
    case Some(obj) if obj.size === 1 =>
      val (label, payload) = obj.toIterable.head
      decodeElement(label, payload)
    case _ =>
      Left(DecodingFailure("Each #children entry must be a single-field object", Nil))

private def decodeGroupedChildren(label: String, json: Json): Decoder.Result[List[Node]] =
  json.asArray match
    case Some(xs) =>
      xs.toList.foldRight(Right(List.empty[Node]): Decoder.Result[List[Node]]) { (j, acc) =>
        for
          tail <- acc
          elem <- decodeElement(label, j)
        yield elem :: tail
      }
    case None =>
      decodeElement(label, json).map(List(_))

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

    val allFields = attributes ++ textField ++ grouped.toSeq ++ orderedChildrenField
    Json.obj(allFields*)
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
