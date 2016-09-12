package com.dataintuitive.luciuscore.lowlevel

import com.dataintuitive.luciuscore.Model._
import com.dataintuitive.luciuscore.GeneModel._

/**
  * Created by toni on 19/04/16.
  */
object GeneDictionaryFunctions extends Serializable {

  // There may be entries with multiple symbol names
  def splitGeneAnnotationSymbols(in: String, value: String): Array[(String, String)] = {
    val arrayString = in.split("///").map(_.trim)
    return arrayString.flatMap(name => Map(name -> value))
  }

  def createGeneDictionary(genesRdd: Array[GeneAnnotation]):GeneDictionary =  {
    genesRdd
      .flatMap(ga => splitGeneAnnotationSymbols(ga.symbol, ga.probesetid))
      .toMap
  }

}
