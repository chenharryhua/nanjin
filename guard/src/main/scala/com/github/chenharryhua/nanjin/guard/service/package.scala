package com.github.chenharryhua.nanjin.guard

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.string.MatchesRegex

package object service {
  type NameConstraint = Refined[String, MatchesRegex["""^[a-zA-Z0-9_./!@#$%&=<>,:\^\?\-\s\*\+\\]+$"""]]
  object NameConstraint extends RefinedTypeOps[NameConstraint, String] with CatsRefinedTypeOpsSyntax
}
