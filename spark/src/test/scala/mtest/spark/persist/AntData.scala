package mtest.spark.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object AntData {

  val ants = List(
    Ant(List(1), Vector(Legs("a", 1), Legs("b", 2))),
    Ant(List(1, 2), Vector(Legs("a", 1), Legs("b", 2))),
    Ant(List(1, 2, 3), Vector(Legs("a", 1), Legs("b", 2)))
  )

  val rdd: RDD[Ant] = sparkSession.sparkContext.parallelize(ants)

  val ds: Dataset[Ant] = Ant.ate.normalize(rdd).dataset

}
