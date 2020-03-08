package com.github.chenharryhua.nanjin

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.boolean.And
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.string.{Trimmed, Uri}
import eu.timepit.refined.types.net

package object database {
  type Username = String Refined And[NonEmpty, Trimmed]
  object Username extends RefinedTypeOps[Username, String] with CatsRefinedTypeOpsSyntax

  type Password = String Refined And[NonEmpty, Trimmed]

  object Password extends RefinedTypeOps[Password, String] with CatsRefinedTypeOpsSyntax {
    override def toString: String = "*****"
  }

  type DatabaseName = String Refined And[NonEmpty, Trimmed]
  object DatabaseName extends RefinedTypeOps[DatabaseName, String] with CatsRefinedTypeOpsSyntax

  type TableName = String Refined And[NonEmpty, Trimmed]
  object TableName extends RefinedTypeOps[TableName, String] with CatsRefinedTypeOpsSyntax

  type Host = String Refined Uri
  object Host extends RefinedTypeOps[Host, String] with CatsRefinedTypeOpsSyntax

  type Port = net.PortNumber
  final val Port = net.PortNumber
}
