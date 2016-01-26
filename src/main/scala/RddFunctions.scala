package LuciusBack

//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.SparkContext._

import LuciusBack.PlateWellIdFunctions.PlateWellId

import scala.util.Try
import org.apache.spark._
import org.apache.spark.rdd._
import L1000._
import GeneFunctions._
import CompoundFunctions._
import FeatureFunctions._
import AnnotationFunctions._
import scala.reflect._

case class GenesPwidsData(genes: Genes,
                              pwids: RDD[PlateWellId],
                              data: RDD[ValueVector])
case class FeaturesPwidsAnnotations(features: Features,
                                        pwids: RDD[PlateWellId],
                                        annotations: RDD[Annotations])

// This datastructure is used for containing the ranks with annotations.
// It is map'ed over when calculating Zhang scores and such
case class AnnotatedRanks(val id:String,
                          val idx:Long,
                          val ranks:Array[Double],
                          val jnjs:String,
                          val jnjb:String,
                          val smiles:String,
                          val inchikey:String,
                          val compoundname:String,
                          val Type:String,
                          val targets:String,
                          val batch:String,
                          val plateid:String,
                          val well:String,
                          val protocolname:String,
                          val concentration:String,
                          val year:String)

// This datastructure contains
case class AnnotatedTPStats(val id:String,
                            val idx:Long,
                            val t:Array[Double],
                            val p:Array[Double],
                            val jnjs:String,
                            val jnjb:String,
                            val smiles:String,
                            val inchikey:String,
                            val compoundname:String,
                            val Type:String,
                            val targets:String,
                            val batch:String,
                            val plateid:String,
                            val well:String,
                            val protocolname:String,
                            val concentration:String,
                            val year:String)

object RddFunctions {

  // The output of this is an RDD with arrays of pairs: (t,p) at every cmpd-gene.
  def joinAndZip(one:RDD[ValueVector], two:RDD[ValueVector]):RDD[Array[Tuple2[Double,Double]]] = {
    return one.zipWithIndex.map{case (x,i) => (i,x)}.join(two.zipWithIndex.map{case (x,i) => (i,x)}).
      sortByKey().
      map{case (i, (t,p)) => t.zip(p)}
  }

  def transpose(rdd:RDD[ValueVector]):RDD[ValueVector] = {
    val transposed = ( rdd.zipWithIndex()
      flatMap{ case (row, row_idx) => row.zipWithIndex.map{ case (el, col_idx) => (col_idx, (row_idx, el)) } }
      groupBy(_._1)
      sortBy(_._1)
      map{ case (_, els) => els.map(_._2).toList.sortBy(_._1) }
      map( row => row.map(tuple => tuple._2))
      map( _.toArray )
      )
    return(transposed)
  }

  // The zip RDD only works for equal length RDDs AND equal length partitions
  // This is good for datastructures where one is a map of the other. Not so good in all other cases.
  // Join is the answer, but requires some bookkeeping. The below makes life a little easier.
  def workingZip[T,U](left:RDD[T], right:RDD[U])(implicit arg0: ClassTag[T], arg1: ClassTag[U]):RDD[(T,U)] = {
    val indexedLeft  = left.zipWithIndex.map(_.swap)
    val indexedRight = right.zipWithIndex.map(_.swap)
    indexedLeft.join(indexedRight).sortBy(_._1).values
  }

  // A variation of the above. The second argument is an RDD of tuples.
  // The result is flattened.
  def workingZipWithTuple[T,U,V](left:RDD[T], right:RDD[(U,V)])(implicit arg0: ClassTag[T], arg1: ClassTag[U], arg2: ClassTag[V]):RDD[(T,U,V)] = {
    val indexedLeft  = left.zipWithIndex.map(_.swap)
    val indexedRight = right.zipWithIndex.map(_.swap)
    indexedLeft.join(indexedRight).sortBy(_._1).values.map{case(i,(j,k)) => (i,j,k)}
  }

  def workingZipWithTriple[T,U,V,W](left:RDD[T], right:RDD[(U,V,W)])(implicit arg0: ClassTag[T], arg1: ClassTag[U], arg2: ClassTag[V],arg3: ClassTag[W]):RDD[(T,U,V,W)] = {
    val indexedLeft  = left.zipWithIndex.map(_.swap)
    val indexedRight = right.zipWithIndex.map(_.swap)
    indexedLeft.join(indexedRight).sortBy(_._1).values.map{case(i,(j,k,l)) => (i,j,k,l)}
  }

  def loadGenesPwidsValues(sc: SparkContext,
                             input_file:String,
                             delimiter:String = "\t",
                             transpose_values:Boolean = true):GenesPwidsData = {
    val raw_rdd = sc.textFile(input_file)
    val pwids = raw_rdd.zipWithIndex.filter{ case (x,i) => i==0 }.map(_._1).flatMap(_.split("\t").drop(1)).cache
    val columns_rdd = raw_rdd.zipWithIndex.filter{ case (x,i) => i!=0 }.map(_._1).map(_.split(delimiter)).cache
    val genes = columns_rdd.map(_(0)).collect
    val values_rdd = columns_rdd.map(_.drop(1).map{x => Try { x.toDouble }.toOption.getOrElse(0.)})
    val cached_values_rdd = {
      if (transpose_values) transpose(values_rdd).cache
      else values_rdd.cache
    }
    GenesPwidsData(genes, pwids, cached_values_rdd)
  }

  def loadFeaturesPwidsAnnotations(sc: SparkContext,
                                     input_file:String,
                                     delimiter:String = "\t"):FeaturesPwidsAnnotations = {
    val raw_rdd = sc.textFile(input_file)
    val features = raw_rdd.first.split(delimiter).drop(1)
    val columns_rdd = raw_rdd.zipWithIndex.filter{ case (x,i) => i!=0 }.map(_._1).map(_.split(delimiter)).cache
    val pwids = raw_rdd.zipWithIndex.filter{ case (x,i) => i==0 }.map(_._1).flatMap(_.split("\t").drop(1)).cache
    val cached_annotations_rdd = columns_rdd.cache
    FeaturesPwidsAnnotations(features, pwids, cached_annotations_rdd)
  }

  def loadGeneTranslationTable(sc: SparkContext,
                             input_file:String,
                             delimiter:String = "\t"):RDD[(String,String)] = {
    val raw_rdd = sc.textFile(input_file)
    val columns_rdd = raw_rdd.zipWithIndex.filter{ case (x,i) => i!=0 }.map(_._1).map(_.split(delimiter)).cache
    val translation_table = columns_rdd.flatMap(line => Map(line(0) -> line(0)) ++ split_symbols(line(3),line(0))).cache
    return translation_table
  }

  // Zipping is sort of an issue with RDDs, because with the transpose, we can't guarantee
  // the same amount of elements in every RDD partition. Bummer!
  // This is a workaround using join instead of zip.
  // It enables things like:
  //    aRanks.groupBy(_.Type).keys.collect
  //    aRanks.groupBy(_.plateid).count
  //    aRanks.filter(_.year == "2013").count
  def joinPwidsRanksAnnotations(pwids:RDD[PlateWellId], ranks:RDD[ValueVector], annotations:RDD[Annotations]) = {
    val indexedPwids = pwids.zipWithIndex.map(_.swap)
    val indexedRanks = ranks.zipWithIndex.map(_.swap)
    val indexedAnnotations = annotations.zipWithIndex.map(_.swap)
    indexedPwids
      .join(indexedRanks)
      .join(indexedAnnotations)
      .map{case (i, ((c,r),a)) => (i, (c, r, a))}
      .map{case (i, (c, r, p)) =>
            AnnotatedRanks(c, i, r, p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13) )}
  }

  // Creating the other relevant RDD for deriving the signatures for combinations of PlateWellIDs (related to 1 compound)
  def joinPwidsTPAnnotations(pwids:RDD[PlateWellId], tStats:RDD[ValueVector], pStats:RDD[ValueVector], annotations:RDD[Annotations]) = {
    val annotatedTPStats1 = workingZipWithTriple(pwids, workingZipWithTuple(annotations, workingZip(tStats, pStats)))
    annotatedTPStats1.zipWithIndex.map {
      case ((c, pheno, t, p),i) =>
        AnnotatedTPStats(c, i, t, p, pheno(1), pheno(2), pheno(3), pheno(4), pheno(5),
          pheno(6), pheno(7), pheno(8), pheno(9), pheno(10), pheno(11), pheno(12), pheno(13))
    }
  }

}
