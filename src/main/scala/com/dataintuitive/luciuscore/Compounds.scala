package com.dataintuitive.luciuscore

import org.apache.spark.rdd.RDD

/**
  * Created by toni on 20/04/16.
  */
class Compound(val compoundJnjs: String,
               val compoundJnjb: String,
               val compoundSmiles: String,
               val compoundInchikey: String,
               val compoundName: String,
               val compoundType: String,
               val compoundTargets: String) extends Serializable {

  val jnjs = compoundJnjs
  val jnjb = compoundJnjb
  val smiles = compoundSmiles
  val inchikey = compoundInchikey
  val name = compoundName
  val ctype = compoundType
  val targets = compoundTargets

  override def toString = s"${compoundName} with inchikey ${compoundInchikey}"

}

class Compounds(val asRdd: RDD[Compound])

object Compounds {

  def apply(relations: SampleCompoundRelations):Compounds = {
    val compounds = relations
      .asRdd
      .groupBy(_.compound.jnjs)
      .map {
        case (jnjs, iterableRelations) => iterableRelations.map(_.compound).head
      }
    new Compounds(compounds)
  }

  // There's room here for a second constructor when entering the name of the relations file

}
