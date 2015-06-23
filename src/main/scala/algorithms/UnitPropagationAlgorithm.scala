package algorithms

import algorithms.types.AlgorithmData
import domain.{Clause, Formula}
import utils.{SatLoggingUtils, ISatCallback}

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
object UnitPropagationAlgorithm extends AbstractAlgorithm[(Formula, List[Int])] with SatLoggingUtils{
  /**
   * Applies the algorithm given the imputs and calls @callback when a solution
   * is found.
   * Returns (new formula, fixed literals) (if any)
   */
  override def applyAlgorithm(upData: AlgorithmData): (Formula, List[Int]) = {
    val formula: Formula = new Formula(upData.formula.n, upData.formula.m)
    var newFixed: List[Int] = upData.fixed

    for (cl <- upData.formula.clauses) {
      var clause: Clause = new Clause()
      var addClause = true

      for (clLit <- cl.literals if addClause) {
        if (newFixed.contains(clLit)) {
          //do not add clause ==> its already true
          addClause = false
        } else if (newFixed.contains(-clLit)) {
          //there is the reverse literal value ==> remove the literal from clause
        } else {
          //add the current literal
          clause.literals ++= List(clLit)
        }
      }

      if (addClause) {
        formula.clauses = formula.clauses ++ List(clause)
        for (l <- clause.literals)
          formula.addClauseOfVar(l, clause)
      }
    }

    var clausesToRemove = List[Clause]()
    var fixedLitsLog = List[Int]()

    for (cl2 <- formula.clauses) {
      if (cl2.literals.size == 1 && newFixed.contains(-cl2.literals.head)) {
        //only one literal remaining in the clause and the literal is fixed negated ==> Formula false!!
        return (null, newFixed)
      } else if (cl2.literals.size == 1) {
        //only one literal remaining in the clause and the literal is not fixed
        clausesToRemove = clausesToRemove ++ List(cl2)
        //if the literal
        if (!newFixed.contains(cl2.literals.head)) {
          newFixed ++= List(cl2.literals.head)
          fixedLitsLog ++= List(cl2.literals.head)
        }
      }
    }

    log.info(
      s"""
         |%%% Unit propagation: Clauses to remove: ${for( c <- clausesToRemove) {c.literals.toString() + " | " }}
          |fixed: ${upData.fixed}
          |new: ${fixedLitsLog}
           """.stripMargin)

    formula.clauses = formula.clauses.filter(p => clausesToRemove.contains(p))

    return (formula, newFixed)
  }
}
