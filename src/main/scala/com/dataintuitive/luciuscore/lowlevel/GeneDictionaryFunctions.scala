package com.dataintuitive.luciuscore.lowlevel

import com.dataintuitive.luciuscore._

/**
  * Created by toni on 19/04/16.
  */
object GeneDictionaryFunctions extends Serializable {

  // There may be entries with multiple symbol names
  def splitGeneAnnotationSymbols(in: String, value: String): Array[(String, String)] = {
    val arrayString = in.split("///").map(_.trim)
    return arrayString.flatMap(name => Map(name -> value))
  }

  def createGeneDictionary(genesRdd: Array[Gene]):GeneDictionary =  {
    genesRdd
      .flatMap(ga => splitGeneAnnotationSymbols(ga.symbol, ga.probesetid))
      .toMap
  }

}