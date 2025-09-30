package com.github.chenharryhua.nanjin.spark.persist

import mtest.spark.*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import shapeless.Coproduct

object CopData {

  lazy val emCops: List[EmCop] = List(
    EmCop(1, EnumCoproduct.Domestic),
    EmCop(2, EnumCoproduct.International)
  )
  import sparkSession.implicits.*
  lazy val emRDD: RDD[EmCop] = sparkSession.sparkContext.parallelize(emCops)
  lazy val emDS: Dataset[EmCop] = sparkSession.createDataset(emRDD)

  lazy val coCops: List[CoCop] = List(
    CoCop(1, CaseObjectCop.Domestic),
    CoCop(2, CaseObjectCop.International)
  )
  lazy val coRDD: RDD[CoCop] = sparkSession.sparkContext.parallelize(coCops)
  lazy val coDS: Dataset[CoCop] = sparkSession.createDataset(coRDD)

  lazy val cpCops = List(
    CpCop(1, Coproduct[CoproductCop.Cop](CoproductCop.Domestic())),
    CpCop(2, Coproduct[CoproductCop.Cop](CoproductCop.International()))
  )

  lazy val cpRDD: RDD[CpCop] = sparkSession.sparkContext.parallelize(cpCops)
  lazy val cpDS: Dataset[CpCop] = sparkSession.createDataset(cpRDD)

}
