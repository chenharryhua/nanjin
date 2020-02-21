package com.github.chenharryhua.nanjin

package object spark extends DatasetExtensions with AvroableDataSource with AvroableDataSink {
  object injection extends InjectionInstances
}
