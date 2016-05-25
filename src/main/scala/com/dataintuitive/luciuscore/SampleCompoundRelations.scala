package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.lowlevel.ioFunctions.loadTsv

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


class SampleCompoundRelation(val sample:Sample,
                             val compound: Compound) extends Serializable {

  override def toString = s"${sample} <-> ${compound}"

}


class SampleCompoundRelations(val sampleCompoundAnnotationsRdd: RDD[SampleCompoundRelation]) extends Serializable {

  val dictionaryRdd = createDictionary
  val inverseDictionaryRdd = dictionaryRdd.map(_.swap)
  val asRdd = sampleCompoundAnnotationsRdd.cache

  def createDictionary = {
    asRdd
      .map(x => (x.sample.samplePwid, x.compound.compoundJnjs))
  }

  def asRddKV = asRdd.map(x => (x.sample.samplePwid, x))

}

object SampleCompoundRelations {

  def apply(sc: SparkContext,
            sampleCompoundAnnotationsFile: String,
            delimiter: String = "\t"):SampleCompoundRelations = {

    val splitSampleCompoundAnnotationsRdd = loadTsv(sc, sampleCompoundAnnotationsFile, delimiter)

    // Turn into RDD containing objects
    val sampleCompoundAnnotationsRdd: RDD[SampleCompoundRelation] =
      splitSampleCompoundAnnotationsRdd
        .map {
          x =>
            val sampleAnnotation = new Sample(x(0), x(8), x(9), x(10), x(11), x(12), x(13))
            val compoundAnnotation = new Compound(x(1), x(2), x(3), x(4), x(5), x(6), x(7))
            new SampleCompoundRelation(sampleAnnotation, compoundAnnotation)
        }

    new SampleCompoundRelations(sampleCompoundAnnotationsRdd)
  }
}