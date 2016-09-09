package com.dataintuitive.luciuscore.utils

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
  * Created by toni on 09/09/16.
  */
class PimpedRDD[V](val rdd:RDD[V]) {

  // The keyed method in its abstract form requires the resulting types to be known
  // This means you need to specify something like this:
  //   val keyedRDD:RDD[(Option[String], DbRow)] = updateDB3.keyed
  //   keyedRDD.first
  // TODO --- look deeper into type specificiations  so this can be resolved with 1 method
  def keyed[K:ClassTag](implicit keyF: V => K):RDD[(K,V)] = rdd.keyBy[K](keyF)

  // This one works well for most aspects of the data, but not e.g. for combinations of features and such!
  // TODO --- look deeper into type specificiations  so this can be resolved with 1 method
  def keyed(implicit keyF: V => Option[String]):RDD[(Option[String],V)] = rdd.keyBy(keyF)

  // Again an overloaded function,
  // TODO --- look deeper into type specificiations  so this can be resolved with 1 method
  def keyed2(implicit keyF: V => (Option[String], Option[String])):RDD[((Option[String], Option[String]),V)] = rdd.keyBy(keyF)

}
