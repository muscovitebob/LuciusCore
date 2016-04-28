package com.dataintuitive.luciuscore.lowlevel

import org.apache.spark.rdd.RDD

/**
  * Created by toni on 20/04/16.
  */
object VectorFunctions {

  def transpose(rdd:RDD[Array[String]]):RDD[Array[String]] = {
    rdd
      .zipWithIndex()
      .flatMap{ case (row, row_idx) => row.zipWithIndex.map{ case (el, col_idx) => (col_idx, (row_idx, el)) } }
      .groupBy(_._1)
      .sortBy(_._1)
      .map{ case (_, els) => els.map(_._2).toList.sortBy(_._1) }
      .map( row => row.map(tuple => tuple._2))
      .map( _.toArray )
  }

}
