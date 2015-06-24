package utils

import domain.{Clause, Formula}

/**
 * Created by mbarreto on 6/22/15.
 */
object SatTestUtils {

  /**
   * 1 3 -4
   * 2 -5 -4
   * -1 -2 -4
   * -5 3 -4
   * 3 4 5
   * @return
   */
  def createFormula : Formula = {
    var formula = new Formula(5, 5)
    for(i <- 0 until 5){
      var clause = new Clause()
      i match {
        case 0 => {
          clause.literals = List(1, 3, -4)
          formula.clauses ++= List(clause)
          formula.addClauseOfVar(1, clause)
          formula.addClauseOfVar(3, clause)
          formula.addClauseOfVar(4, clause)
        }
        case 1 => {
          clause.literals = List(2, -5, -4)
          formula.clauses ++= List(clause)
          formula.addClauseOfVar(2, clause)
          formula.addClauseOfVar(5, clause)
          formula.addClauseOfVar(4, clause)
        }
        case 2 => {
          clause.literals = List(-1, -2, -4)
          formula.clauses ++= List(clause)
          formula.addClauseOfVar(1, clause)
          formula.addClauseOfVar(2, clause)
          formula.addClauseOfVar(4, clause)
        }
        case 3 => {
          clause.literals = List(-5, 3, -4)
          formula.clauses ++= List(clause)
          formula.addClauseOfVar(5, clause)
          formula.addClauseOfVar(3, clause)
          formula.addClauseOfVar(4, clause)
        }
        case 4 => {
          clause.literals = List(3, 4, 5)
          formula.clauses ++= List(clause)
          formula.addClauseOfVar(3, clause)
          formula.addClauseOfVar(4, clause)
          formula.addClauseOfVar(5, clause)
        }
      }
    }

    return formula
  }
}
