package com.dataintuitive.luciuscore.Experiments

import ExperimentalModel.{DbRow, Sample, SampleAnnotations}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scalaz._

/**
  * Created by toni on 15/06/16.
  */
object Lenses extends Serializable {

  // Given an RDD (from a source file) that contains an array of strings in each row,
  // retrieve as key-value pairs
  def retrieveKeyValues(rdd:RDD[Array[String]], key:String, values:List[String]) = {
    val header = rdd.first
    val keyIndex = header.indexOf(key)
    val valueIndices = values.map(value => header.indexOf(value))
    rdd
      .map(row => (row.lift(keyIndex), valueIndices.map(valueIndex => row.lift(valueIndex)).toArray))
  }

  // Given an RDD (from a source file) that contains an array of strings in each row,
  // retrieve only specific rows, based on the header name.
  def retrieveArray(rdd:RDD[Array[String]], features:Seq[String], includeHeader:Boolean = false) = {
    val header = rdd.first
    val featureIndices = features.map(value => header.indexOf(value))
    val selectionRdd = rdd
      .map(row => featureIndices.map(valueIndex => row.lift(valueIndex)).toArray)
    if (!includeHeader)
      selectionRdd.zipWithIndex.filter(_._2 > 0).keys
    else
      selectionRdd
  }

  // Given two RDDs and a function for their keys, 'update' the first by adding information from the second.
  def updateByLens[K: ClassTag, V: ClassTag, W: ClassTag](rdd1:RDD[V], keyF1: V => K)
                                                         (rdd2:RDD[W], keyF2: W => K)
                                                         (updateF:(V,W) => V):RDD[V] = {
    val rdd1ByKey = rdd1.keyBy(keyF1)
    val rdd2ByKey:RDD[(K,W)] = rdd2.keyBy(keyF2)
    //rdd1ByKey.join(rdd2ByKey)
    rdd1ByKey.join(rdd2ByKey).values.map{case (source, update) => updateF(source, update)}
  }



}
