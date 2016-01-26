package LuciusBack

import LuciusBack.L1000.OrderedSignature

object GeneFunctions {

  type Gene = String
  type Genes = Array[Gene]
  type GeneDict = Map[Gene,Int]

  type TranslationTable = Map[String,String]

  //  Take the sign of a gene in a signature
  def signGene(s:String):String =
    if (s.startsWith("-")) "-"
    else ""

  //  Take the absolute value of a gene in a signature
  def absGene(s:String):String =
    if (s.startsWith("-")) s.drop(1)
    else s

  // Translate the gene names to the common standard. Take into account anti-correlation
  // The output is an array, as this is the preferred way to continue.
  def translateGenes(signature:OrderedSignature, tt: TranslationTable): OrderedSignature = {
    signature.map{gene => signGene(gene) + tt.getOrElse(absGene(gene), "OOPS") }
      .toArray
  }

  // There may be entries with multiple symbol names
  def split_symbols(in:String, value:String):Array[(String,String)] = {
    val array_string = in.split("///").map(_.trim)
    return array_string.flatMap(name => Map(name -> value))
  }


}
