package algorithms

import algorithms.types.AlgorithmData
import common.SatMapReduceHelper
import domain.{Clause, Formula}
import utils.{SatLoggingUtils, ISatCallback}

import scala.util.Random

/**
 * Created by mbarreto on 3/8/15.
 */
object SchoningAlgorithm extends AbstractAlgorithm[List[Int]] with SatLoggingUtils {

  /**
   * Implements the Schoning algorithm.
   * @return list of fixed literals if a solution is found otherwise returns null.
   */
  override def applyAlgorithm(upData: AlgorithmData): List[Int] = {
    val randomGen = new Random()
    var fixedLiterals = List[Int]()
    var notFixedLiterals = getNotFixedLiterals(upData)
    log.info(s"Fixed Literals: ${upData.fixed}, notfixed: ${notFixedLiterals}")

    for (i <- 0 to upData.formula.n) {
      //literal selection
      fixedLiterals = List[Int]()
      //create a binary selection of (not fixed ints)
      val binarySelection = SatMapReduceHelper.toBinary(randomGen.nextInt(), upData.formula.n - upData.fixed.size)
      //generate the assignment
      val partialAssignment = SatMapReduceHelper.createMap(notFixedLiterals, binarySelection)
      var newAssignment = partialAssignment ++ upData.fixed
      log.info(s"""
             Schoning --> newAssignment: ${newAssignment}""")
      if (upData.formula.isSatisfasiable(newAssignment)) {
        //solution found!!
        log.info(s"Solution Found in SCHONNING: ${newAssignment.toString()}")
        return newAssignment
      } else {
        var firstFalseClauseFound: Boolean = false
        for (j <- 0 until (3 * upData.formula.n)) {
          firstFalseClauseFound = false
          for (clause <- upData.formula.clauses if !firstFalseClauseFound) {
            if (!clause.isSatisfasiable(newAssignment)) {
              //first false clause found! ==> flip one literal
              newAssignment = SchoningAlgorithm.flipLiteralOfClause(clause, newAssignment, selectRandomLiteral(clause))
              firstFalseClauseFound = true
            }
          }
          if (!firstFalseClauseFound && upData.formula.isSatisfasiable(newAssignment)) {
            //the assignment is a solution!
            log.info(s"[SCHO] Schonning solution: ${newAssignment}")
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
  private def getNotFixedLiterals(upData: AlgorithmData): List[Int] = {
    var notFixedLiterals = List[Int]()

    upData.formula.getLiteralsInOrder().foreach(l => {
      //l and -l is not fixed
      if (!(upData.fixed contains l) && !(upData.fixed contains -l)) {
        notFixedLiterals = notFixedLiterals ++ List(l)
      }
    })

    return notFixedLiterals
  }

  def selectRandomLiteral(clause: Clause) : Int = Random.nextInt(clause.literals.size)

  def flipLiteralOfClause(clause: Clause, currentAssignment: List[Int], literalIndex : Int): List[Int] = {
    var newAssignment = List[Int]()

    val toFlipLiteral = clause.literals apply literalIndex
    var flipedAssignment = List[Int]()
    for (lit <- currentAssignment) {
      if (lit.equals(toFlipLiteral) || lit.equals(-toFlipLiteral)) {
        flipedAssignment ++= List(-lit)
      } else {
        flipedAssignment ++= List(lit)
      }
    }
    return flipedAssignment;
  }
}
