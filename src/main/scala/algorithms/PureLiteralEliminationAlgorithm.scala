package algorithms

import algorithms.types.AlgorithmData
import domain.{Clause, Formula}
import utils.{SatLoggingUtils, ISatCallback}

/**
 * Created by mbarreto on 3/8/15.
 */
object PureLiteralEliminationAlgorithm extends AbstractAlgorithm[(Formula, List[Int])] with SatLoggingUtils{

  /**
   * Applies the pure literal elimination algorithm to the formula.
   * Given the imputs and calls @callback when a solution
   * is found.
   * Returns (solutions_found, prunned)
   */
  override def applyAlgorithm(upData : AlgorithmData): (Formula, List[Int]) = {
    val formula = new Formula(upData.formula.n, upData.formula.m)
    var literals = upData.fixed
    var litsLog = List[Int]();
    var clausesLog = List[Clause]()

    for (cl <- upData.formula.clauses) {
      if (cl.literals.size == 1){
        clausesLog ++= List(cl)

        if (upData.fixed.contains(- cl.literals.head)){
          //contains the negated values ==> formula false
          return (null, null)
        }else if (upData.fixed.contains(cl.literals.head)){
          //dont add it to the formula as the literal is already fixed
        }else{
          //fix the literal
          literals ++= List(cl.literals.head)
          litsLog ++= List(cl.literals.head)
        }
      }else{
        formula.clauses ++= List(cl)
      }
    }

    log.info(
      s"""
         |%%% PureLitearlElim --> fixed: ${upData.fixed.toString}, literals: ${litsLog.toString()}
         |Removed Clauses: ${for(c <- clausesLog) c.toString + "|"}
       """.stripMargin)

    return (formula, literals)
  }

}
