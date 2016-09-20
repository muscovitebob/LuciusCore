package com.dataintuitive.luciuscore.utilities

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Various functions related to `RDD`'s combined.
  */
object RddFunctions {

  def transpose[T:ClassTag](rdd:RDD[Array[T]]):RDD[Array[T]] = {
    rdd
      .zipWithIndex()
      .flatMap{ case (row, row_idx) => row.zipWithIndex.map{ case (el, col_idx) => (col_idx, (row_idx, el)) } }
      .groupBy(_._1)
      .sortBy(_._1)
      .map{ case (_, els) => els.map(_._2).toList.sortBy(_._1) }
      .map( row => row.map(tuple => tuple._2))
      .map( _.toArray )
  }


  // Given two RDDs and a function for their keys, 'update' the first by adding information from the second.
  // The order of the to-be-joined RDDs is important as we are using a lefOutJoin here!
  // Please note that the update function could be replaced by Lens (see e.g. scalaz)
  def updateAndTransformRDD[K: ClassTag, V: ClassTag, W: ClassTag, X: ClassTag](rdd1:RDD[V],
                                                                                keyF1: V => K,
                                                                                rdd2:RDD[W],
                                                                                keyF2: W => K)(
                                                                                 update:(V,X) => V)(
                                                                                 transform:W => X = identity[W] _):RDD[V] = {
    val rdd1ByKey = rdd1.keyBy(keyF1)
    val rdd2ByKey:RDD[(K,W)] = rdd2.keyBy(keyF2)
    rdd1ByKey.leftOuterJoin(rdd2ByKey).values.map{case (source, upd) =>
      upd match {
        case Some(u) => update(source, transform(u))
        case None    => source
      }
    }
  }

  /* Curried version of the above function, for defining transformations in sequence
  */
  def joinUpdateRDD[V: ClassTag,K:ClassTag,W](
                                               rdd2:RDD[(K, W)], update:(V,W) => V)(rdd:RDD[(K,V)]):RDD[(K,V)] = {

    rdd.leftOuterJoin(rdd2).map{case (key, (source, upd)) =>
      upd match {
        case Some(u) => (key, update(source, u))
        case None    => (key, source)
      }
    }
  }

  /* Curried version of the above function, for defining transformations in sequence
  */
  def joinUpdateTransformRDD[V: ClassTag,K:ClassTag,W,X](rdd2:RDD[(K, W)], update:(V,X) => V, transform:W => X)(rdd:RDD[(K,V)]):RDD[(K,V)] = {

    rdd.leftOuterJoin(rdd2).map{case (key,(source, upd)) =>
      upd match {
        case Some(u) => (key, update(source, transform(u)))
        case None    => (key, source)
      }
    }
  }





}
