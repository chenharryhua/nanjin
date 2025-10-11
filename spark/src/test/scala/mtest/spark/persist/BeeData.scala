package mtest.spark.persist

import mtest.spark.*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object BeeData {

  val bees = List(Bee(Array(1, 2, 3), 1), Bee(Array(2, 3, 4), 2), Bee(Array(), 3))

  val rdd: RDD[Bee] = sparkSession.sparkContext.parallelize(bees)
  import sparkSession.implicits.*

  val ds: Dataset[Bee] = sparkSession.createDataset(rdd)
}
