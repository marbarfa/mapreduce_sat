package algorithms

import algorithms.types.AlgorithmData
import common.SatMapReduceHelper
import domain.Formula
import utils.{SatLoggingUtils, ISatCallback}

import scala.util.Random

/**
 * Created by mbarreto on 3/8/15.
 */
object SchoningAlgorithm extends AbstractAlgorithm [List[Int]] with SatLoggingUtils {

  /**
   * Implements the Schoning algorithm.
   * @return list of fixed literals if a solution is found otherwise returns null.
   */
  override def applyAlgorithm(upData : AlgorithmData): List[Int] = {
    val randomGen = new Random()
    var fixedLiterals = List[Int]()
    var notFixedLiterals = getNotFixedLiterals(upData)

    for(i <- 0 to upData.formula.n) {
      //literal selection
      fixedLiterals = List[Int]()
      //get a new random number
      var randNumber = randomGen.nextInt(Math.pow(2.0, upData.formula.n.toDouble).toInt + 1)
      //create a binary selection of (not fixed ints)
      val binarySelection = SatMapReduceHelper.toBinary(randNumber, upData.formula.n - upData.fixed.size)
      //generate the assignment
      val partialAssignment = SatMapReduceHelper.createMap(notFixedLiterals, binarySelection)
      var newAssignment = partialAssignment ++ upData.fixed
      if (upData.formula.isSatisfasiable(newAssignment, log)){
        //solution found!!
        return newAssignment
      }else {
        for(j <- 0 to (3 * upData.formula.n)) {
          var firstFalseClauseFound = false
          var index = 0
          for(clause <- upData.formula.clauses if !firstFalseClauseFound) {

            if (!clause.isSatisfasiable(newAssignment)){
              //first false clause found! ==> flip one literal
              val ranLit = Random.nextInt(3)
              val toFlipLiteral = clause.literals apply ranLit
              var flipedAssignment = List[Int]()
              newAssignment  = newAssignment.slice(0, index) ++ List(-toFlipLiteral) ++ newAssignment.slice(index+1, newAssignment.size)

              firstFalseClauseFound = true
            }
            index +=1

          }

          if (!firstFalseClauseFound){
            //the assignment is a solution!
            return newAssignment
          }
        }
      }
    }

    //no solution found
    return null
  }

  /**
   * Returns the literals that have not been yet fixed.
   * @param upData
   * @return
   */
  private def getNotFixedLiterals(upData : AlgorithmData) : List[Int] = {
    var notFixedLiterals = List[Int]()

    upData.formula.literalsInOrder.foreach( l => {
      //l and -l is not fixed
      if (!(upData.fixed contains l) && !(upData.fixed contains (-l))){
        notFixedLiterals = notFixedLiterals ++ List(l)
      }
    })

    return notFixedLiterals
  }

}
