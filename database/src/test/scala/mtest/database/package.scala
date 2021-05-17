package mtest

import com.github.chenharryhua.nanjin.common.database._
import com.github.chenharryhua.nanjin.database.Postgres

package object database {

  val postgres: Postgres =
    Postgres(Username("postgres"), Password("postgres"), Host("localhost"), Port(5432), DatabaseName("postgres"))

}
