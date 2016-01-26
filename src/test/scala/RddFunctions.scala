package LuciusBack

import LuciusBack.RddFunctions._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

class RddFunctionsTest extends FlatSpec {

  val conf = new SparkConf().setAppName("L1000UnitTests").setMaster("local")
  val sc = new SparkContext(conf)

  info("Testing RddFunctions...")

  "Simple transpose of transpose" should "return original dataset" in {
    val a:RDD[Array[Double]] = sc.parallelize(Array(Array(1.,2.,3.),Array(4.,5.,6.),Array(7.,8.,9.)))
    val att:RDD[Array[Double]] = transpose(transpose(a))
    assert( att.collect === a.collect )
  }

  // TODO: Write some more tests for the other functions!


}
