package mtest.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.database.*
import com.github.chenharryhua.nanjin.database.*
import com.github.chenharryhua.nanjin.spark.*
package object database {

  val postgres: Postgres =
    Postgres(Username("postgres"), Password("postgres"), Host("localhost"), Port(5432), DatabaseName("postgres"))

  val sparkDB: SparkDBContext[IO] = sparkSession.alongWith[IO](postgres.hikariConfig)
}
