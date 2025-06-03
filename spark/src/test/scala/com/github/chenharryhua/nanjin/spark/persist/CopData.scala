package com.github.chenharryhua.nanjin.spark.persist

import frameless.TypedDataset
import mtest.spark.*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import shapeless.Coproduct

object CopData {

  implicit private val ss: SparkSession = sparkSession
  val emCops: List[EmCop] = List(
    EmCop(1, EnumCoproduct.Domestic),
    EmCop(2, EnumCoproduct.International)
  )
  val emRDD: RDD[EmCop] = sparkSession.sparkContext.parallelize(emCops)
  val emDS: Dataset[EmCop] = TypedDataset.create(emRDD).dataset

  val coCops: List[CoCop] = List(
    CoCop(1, CaseObjectCop.Domestic),
    CoCop(2, CaseObjectCop.International)
  )
  val coRDD: RDD[CoCop] = sparkSession.sparkContext.parallelize(coCops)
  val coDS: Dataset[CoCop] = TypedDataset.create(coRDD).dataset

  val cpCops = List(
    CpCop(1, Coproduct[CoproductCop.Cop](CoproductCop.Domestic())),
    CpCop(2, Coproduct[CoproductCop.Cop](CoproductCop.International()))
  )

  val cpRDD: RDD[CpCop] = sparkSession.sparkContext.parallelize(cpCops)
  val cpDS: Dataset[CpCop] = TypedDataset.create(cpRDD).dataset

}
