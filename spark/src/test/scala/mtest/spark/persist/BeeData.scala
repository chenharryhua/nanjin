package mtest.spark.persist

import org.apache.spark.rdd.RDD

object BeeData {

  val bees = List(Bee(Array(1, 2, 3), 1), Bee(Array(2, 3, 4), 2), Bee(Array(), 3))

  val rdd: RDD[Bee] = sparkSession.sparkContext.parallelize(bees)
}
