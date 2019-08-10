package com.dataintuitive.luciuscore.io

import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.SparkContext
import com.dataintuitive.luciuscore.utilities.RddFunctions._
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
  * Load stats from file: t stats and corresponding p stats.
  *
  * Please note that in L1000, usually the data is transposed so that we have to transpose it back before
  * updating the database. This take time.
  */
object StatsIO {

  /**
    * Helper function to load a specific batch into an RDD that later can be union'd with all the others.
    */
  def processStatsBatch(raw: RDD[Array[String]], batch: Int, batchSize: Int):RDD[Array[String]] = {
    // Offset is 1 because we don't need gene names
    val fromIndex = 1 + batch * batchSize
    val tillIndex = 1 + (1 + batch) * batchSize
    val subset = raw.map(_.slice(fromIndex, tillIndex))
    transpose(subset)
  }

  /**
    * Load batches of data in order to make transposition possible.
    * API is the same as before except we have an option for the batch size.
    * TODO: Fix a bug somewhere here that makes this return/save only the first batchSize-1 samples!!!
    */
  def loadStatsFromFileInBatches(sc: SparkContext, fileName:String, toTranspose:Boolean = true, batchSize:Int = 50000):RDD[Array[String]] = {
    val raw = sc.textFile(fileName).map(_.split("\t").map(_.trim))
    // Prepend an empty entry in the header
    val correctedHeader = raw.zipWithIndex.map{case (v, i) => if (i == 0) "" +: v else v}
    if (toTranspose) {
      val nrBatches = correctedHeader.first.length / batchSize
      val results = (0 to nrBatches).map(batch => processStatsBatch(correctedHeader, batch, batchSize))
      results.reduce(_ union _)
    } else {
      raw
    }
  }

  /**
    * Primary entry point to loading t and p statistics. The difference is only made upon updating the database.
    */
  def loadStatsFromFile(sc: SparkContext, fileName:String, toTranspose:Boolean = true):RDD[Array[String]] = {
    val raw = sc.textFile(fileName).map(_.split("\t").map(_.trim))
    if (toTranspose)
      transpose(raw)
    else
      raw
  }

  def statsKey(x:Array[String]):Option[String] = Some(x.head)

  def updateStats(stats:RDD[Array[String]], db:RDD[DbRow], updateF:(DbRow, Array[Double]) => DbRow):RDD[DbRow] = {
    val keyedStats = stats.keyBy(statsKey)
    val statsUpdate = joinUpdateTransformRDD(keyedStats, updateF, transformStats) _
    statsUpdate(db.keyBy(pwidKey)).values
  }

  def dbUpdateT(d:DbRow, update:Array[Double]) = {
    d.copy(
      sampleAnnotations=d.sampleAnnotations.copy(
        t=Some(update)
      )
    )
  }

  def dbUpdateP(d:DbRow, update:Array[Double]) = {
    d.copy(
      sampleAnnotations=d.sampleAnnotations.copy(
        p=Some(update)
      )
    )
  }

  def transformStats(update:Array[String]):Array[Double] = {
    update.drop(1).map(x => Try(x.toDouble).toOption.getOrElse(0.0))
  }

  val pwidKey = (x:DbRow) => x.sampleAnnotations.sample.pwid

}
