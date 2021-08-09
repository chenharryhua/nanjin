package com.github.chenharryhua.nanjin.spark.dstream

import cats.Functor
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import java.time.ZoneId

@Lenses final private[dstream] case class SDParams private (
  zoneId: ZoneId,
  pathBuilder: String => NJTimestamp => String
)

object SDParams {

  private def pathBuilder(zoneId: ZoneId)(path: String)(ts: NJTimestamp): String =
    if (path.endsWith("/"))
      s"$path${ts.`Year=yyyy/Month=mm/Day=dd`(zoneId)}"
    else
      s"$path/${ts.`Year=yyyy/Month=mm/Day=dd`(zoneId)}"

  def apply(zoneId: ZoneId): SDParams =
    SDParams(zoneId = zoneId, pathBuilder = pathBuilder(zoneId))
}

sealed private[dstream] trait SDConfigF[A]

private object SDConfigF {
  implicit val functorSDConfigF: Functor[SDConfigF] = cats.derived.semiauto.functor[SDConfigF]

  final case class InitParams[K](zoneId: ZoneId) extends SDConfigF[K]
  final case class WithPathBuilder[K](f: String => NJTimestamp => String, cont: K) extends SDConfigF[K]

  private val algebra: Algebra[SDConfigF, SDParams] = Algebra[SDConfigF, SDParams] {
    case InitParams(zoneId)    => SDParams(zoneId)
    case WithPathBuilder(v, c) => SDParams.pathBuilder.set(v)(c)
  }
  def evalConfig(cfg: SDConfig): SDParams = scheme.cata(algebra).apply(cfg.value)
}

final private[spark] case class SDConfig private (value: Fix[SDConfigF]) {
  import SDConfigF.*
  def evalConfig: SDParams = SDConfigF.evalConfig(this)

  def pathBuilder(f: String => NJTimestamp => String): SDConfig =
    SDConfig(Fix(WithPathBuilder(f, value)))

}

private[spark] object SDConfig {

  def apply(zoneId: ZoneId): SDConfig =
    SDConfig(Fix(SDConfigF.InitParams[Fix[SDConfigF]](zoneId)))

}
