package com.github.chenharryhua.nanjin.common

import eu.timepit.refined.W
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.string.{MatchesRegex, Url}

object aws {
  type SqsUrl = String Refined Url
  object SqsUrl extends RefinedTypeOps[SqsUrl, String] with CatsRefinedTypeOpsSyntax

  type IamArn = String Refined MatchesRegex[W.`"^arn:aws:iam::\\\\d{12}:role/[A-Za-z0-9_-]+$"`.T]
  object IamArn extends RefinedTypeOps[IamArn, String] with CatsRefinedTypeOpsSyntax

  type SnsArn = String Refined MatchesRegex[W.`"^arn:aws:sns:[A-Za-z0-9_-]+:\\\\d{12}:[A-Za-z0-9_-]+$"`.T]
  object SnsArn extends RefinedTypeOps[SnsArn, String] with CatsRefinedTypeOpsSyntax
}
