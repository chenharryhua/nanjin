package com.github.chenharryhua.nanjin

package object spark extends DatasetExtensions {

  object injection extends InjectionInstances

  private[spark] val SparkDatetimeConversionConstant: Int = 1000
}
