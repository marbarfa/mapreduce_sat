package algorithms

import algorithms.types.AlgorithmData
import domain.{Clause, Formula}
import utils.ISatCallback

/**
 * Implementation of the UnitPropagation algorithm.
 *
 * The UnitPropagation algorithm is applied for each literal assignment.
 * Set of clauses a∨b,¬a∨c,¬c∨d,a
 * Can be simplified by unit propagation of unit clause a a v b: is removed
 * ¬a ∨ c : a is deleted resulting in c
 * ¬c ∨ d : not changed
 * a: not changed -> always true!
 * resulting set: c, ¬c ∨ d
 *
 * Created by mbarreto on 3/8/15.
 */
object UnitPropagationAlgorithm extends AbstractAlgorithm[(Formula, List[Int])] {
  /**
   * Applies the algorithm given the imputs and calls @callback when a solution
   * is found.
   * Returns (new formula, fixed literals) (if any)
   */
  override def applyAlgorithm(upData : AlgorithmData) : (Formula, List[Int]) = {
    val formula : Formula = new Formula()
    var newFixed : List[Int] = List[Int]()

    for (cl <- upData.formula.clauses) {
      var clause : Clause = new Clause()
      var addClause = true
      for(clLit <- cl.literals if addClause){
        if (upData.fixed.contains(clLit)){
          //do not add clause ==> its already true
          addClause = false
        }else if (upData.fixed.contains(-clLit)){
          //there is the reverse literal value ==> remove the literal from clause
        }else {
          //add the current literal
           clause.literals = clause.literals ++ List(clLit)
        }
      }
      if (addClause) {
        formula.clauses = formula.clauses ++ List(clause)
        for (l <- clause.literals)
          formula.addClauseOfVar(l, clause)
      }
      var clausesToRemove = List[Clause]();

      for(cl <- formula.clauses){
        if (cl.literals.size == 1 && upData.fixed.contains(-cl.literals(0))){
          //only one literal remaining in the clause and the literal is fixed negated ==> Formula false!!
          return null
        }else if (cl.literals.size == 1) {
          //only one literal remaining in the clause and the literal is not fixed
          clausesToRemove = clausesToRemove ++ List(cl)
          //if the literal
          if (!upData.fixed.contains(cl.literals.apply(0)))
            newFixed = newFixed ++ List(cl.literals.apply(0))
        }
      }
      formula.clauses = formula.clauses.filter(p => clausesToRemove.contains(p))
    }

    return (formula, newFixed)
  }
}
