package algorithms

import algorithms.types.AlgorithmData
import main.scala.domain.{Clause, Formula}
import main.scala.utils.ISatCallback

/**
 * Created by mbarreto on 3/8/15.
 */
object PureLiteralEliminationAlgorithm extends AbstractAlgorithm[(Formula, List[Int])] {

  /**
   * Applies the algorithm given the imputs and calls @callback when a solution
   * is found.
   * Returns (solutions_found, prunned)
   */
  override def applyAlgorithm(upData : AlgorithmData): (Formula, List[Int]) = {
    val formula = new Formula()
    var literals = List[Int]()

    for (cl <- upData.formula.clauses) {
      if (cl.literals.size == 1){
        if (upData.fixed.contains(- cl.literals.head)){
          //contains the negated values ==> formula false
          return null
        }else if (upData.fixed.contains(cl.literals.head)){
          //dont add it to the formula as the literal is already fixed
        }else{
          //fix the literal
          literals = literals ++ List(cl.literals.head)
        }
      }else{
        formula.clauses = formula.clauses ++ List(cl)
      }
    }
    return (formula, literals)
  }

}
