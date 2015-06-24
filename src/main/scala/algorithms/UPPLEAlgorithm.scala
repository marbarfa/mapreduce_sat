package algorithms

import algorithms.types.AlgorithmData
import domain.{Clause, Formula}
import utils.SatLoggingUtils

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
object UPPLEAlgorithm extends AbstractAlgorithm[(Formula, List[Int])] with SatLoggingUtils{


  def reduceFormula(formula: Formula, literals: List[Int]): Formula = {
    var res = new Formula(formula.n, formula.m)
    for(c <- formula.clauses){
      var clause = new Clause()
      var addClause = true;
      for(l <- c.literals if addClause) {
        if (literals.contains(l)){
          //do not add this clause as true literal is selected
          addClause = false;
        }else if (literals.contains(-l)) {
          //do not add this literal
        }else{
          clause.literals ++= List(l)
        }
      }
      if (addClause){
        res.clauses ++= List(clause)
        for(l <- clause.literals){
          res.addClauseOfVar(l, clause)
        }
      }
    }
    return res
  }

  /**
   * Applies the algorithm given the imputs and calls @callback when a solution
   * is found.
   * Returns (new formula, fixed literals) (if any)
   */
  override def applyAlgorithm(upData: AlgorithmData): (Formula, List[Int]) = {
    var formula = reduceFormula(upData.formula, upData.fixed)
    var newFixed: List[Int] = upData.fixed

    var pureLiteralFound = true
    var pureClause : Clause = null

    do {
      pureLiteralFound = false

      for (cl2 <- formula.clauses if !pureLiteralFound) {
        if (cl2.literals.size == 1) {
          pureLiteralFound = true;
          pureClause = cl2;
        }
      }

      if (pureLiteralFound){
        newFixed ++= List(pureClause.literals.head)
        formula = reduceFormula(formula, newFixed)
      }

    }while(pureLiteralFound);

    return (formula, newFixed)
  }

}
