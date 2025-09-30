package com.github.chenharryhua.nanjin.spark.persist

import mtest.spark.*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object AntData {

  val ants = List(
    Ant(List(1), Vector(Legs("a", 1), Legs("b", 2))),
    Ant(List(1, 2), Vector(Legs("a", 1), Legs("b", 2))),
    Ant(List(1, 2, 3), Vector(Legs("a", 1), Legs("b", 2)))
  )

  val rdd: RDD[Ant] = sparkSession.sparkContext.parallelize(ants)
  import sparkSession.implicits.*

  val ds: Dataset[Ant] = sparkSession.createDataset(rdd)

}
