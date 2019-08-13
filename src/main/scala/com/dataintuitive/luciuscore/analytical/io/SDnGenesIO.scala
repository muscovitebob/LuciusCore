package com.dataintuitive.luciuscore.analytical.io

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType}


object SDnGenesIO {

  case class NumberAndSD(n: Int, SD: Double)
  def readSDnGenes(spark: SparkSession, filepath: String): Map[Int, Double] = {
    import spark.implicits._
    spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", "\t").csv(filepath)
      .withColumn("n", 'n.cast(IntegerType)).withColumn("SD", 'SD.cast(DoubleType))
      .as[NumberAndSD].rdd.collect.map(x => (x.n, x.SD)).toMap
  }

}
