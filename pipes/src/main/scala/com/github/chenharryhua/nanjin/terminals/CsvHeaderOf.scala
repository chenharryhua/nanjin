package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import kantan.csv.CsvConfiguration.Header
import monocle.Monocle.focus
import scala.deriving._
import scala.compiletime.constValueTuple

/** Typeclass for CSV header of a case class `A`. */
sealed trait CsvHeaderOf[A] {

  /** explicit header */
  def header: Header.Explicit

  /** modify each field name with a function `f` */
  final def modify(f: Endo[String]): Header.Explicit =
    header.focus(_.header).modify(_.map(f))
}

object CsvHeaderOf {

  /** summon the typeclass for `A` */
  def apply[A](using ev: CsvHeaderOf[A]): ev.type = ev

  /** internal helper to build CsvHeaderOf without inline anonymous duplication */
  private def fromLabels(labels: List[String]): CsvHeaderOf[Any] =
    new CsvHeaderOf[Any] {
      override val header: Header.Explicit = Header.Explicit(labels)
    }

  /** fully generic derivation for case classes, no duplicate anonymous class warning */
  inline given derived[A](using m: Mirror.ProductOf[A]): CsvHeaderOf[A] =
    fromLabels(
      constValueTuple[m.MirroredElemLabels]
        .productIterator
        .toList
        .asInstanceOf[List[String]]
    ).asInstanceOf[CsvHeaderOf[A]] // scalafix:ok
}
