package com.github.chenharryhua.nanjin

import eu.timepit.refined.W
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.string.{MatchesRegex, Url}

package object aws {
  type SqsUrl = String Refined Url
  object SqsUrl extends RefinedTypeOps[SqsUrl, String] with CatsRefinedTypeOpsSyntax

  type IamArn = String Refined MatchesRegex[W.`"^arn:aws:iam::\\\\d{12}:role/[A-Za-z0-9]+$"`.T]
  object IamArn extends RefinedTypeOps[IamArn, String] with CatsRefinedTypeOpsSyntax

  type SnsArn = String Refined MatchesRegex[W.`"^arn:aws:sns::\\\\d{12}:role/[A-Za-z0-9]+$"`.T]
  object SnsArn extends RefinedTypeOps[SnsArn, String] with CatsRefinedTypeOpsSyntax
}
