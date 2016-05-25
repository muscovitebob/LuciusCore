package com.dataintuitive

/**
  * Created by toni on 28/04/16.
  */
package object luciuscore {

  type Probesetid = String
  type Symbol = String

  type GeneDictionary = Map[Symbol, Probesetid]
  type InverseGeneDictionary = Map[Probesetid,Symbol]

  type NotationType = String
  val SYMBOL = "symbol"
  val PROBESETID = "probesetid"
  val INDEX = "index"


  // For Vector derivatives
  type Value = Double
  type ValueVector = Array[Value]

  type Rank = Double
  type RankVector = Array[Rank]

  type GeneVector = Array[String]

  type SignatureType = GeneVector

  // A set could be a good way to represent an unordered signature
  type UnorderedSignature = Set[String]

  type Index = Int


  // For stats
  type OneD = Tuple2[String, Array[Double]]
  type TP = Tuple3[String, Array[Double], Array[Double]]
  type TPR = Tuple4[String, Array[Double], Array[Double], Array[Double]]
  type ATPR = Tuple6[String, Sample, Compound, Array[Double], Array[Double], Array[Double]]
  type ZATPR = Tuple7[String, Sample, Compound, Array[Double], Array[Double], Array[Double], Double]

  class TPExtension(val x: TP) extends Serializable {
    val pwid = x._1
    val t = x._2
    val p = x._3
  }

  class TPRExtension(val x: TPR) extends Serializable  {
    val pwid = x._1
    val t = x._2
    val p = x._3
    val r = x._4
  }

  class ATPRExtension(val x: ATPR) extends Serializable  {
    val pwid = x._1
    val sample = x._2
    val compound = x._3
    val t = x._4
    val p = x._5
    val r = x._6
  }

  class ZATPRExtension(val x: ZATPR) extends Serializable  {
    val pwid = x._1
    val sample = x._2
    val compound = x._3
    val t = x._4
    val p = x._5
    val r = x._6
    val zhang = x._7
  }


  implicit def extraTP(x:TP) = new TPExtension(x)

  implicit def extraTPR(x:TPR) = new TPRExtension(x)

  implicit def extraATPR(x:ATPR) = new ATPRExtension(x)

  implicit def extraZATPR(x:ZATPR) = new ZATPRExtension(x)
  
}
