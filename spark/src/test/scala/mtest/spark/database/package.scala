package mtest.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.database.*
import com.github.chenharryhua.nanjin.database.*
import com.github.chenharryhua.nanjin.spark.*
import eu.timepit.refined.auto.*
package object database {

  val postgres: Postgres = Postgres(Username("postgres"), Password("postgres"), "localhost", 5432, "postgres")

  val sparkDB: SparkDBContext[IO] = sparkSession.alongWith[IO](NJHikari(postgres))
}
