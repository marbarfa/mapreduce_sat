package domain

import org.scalatest._

/**
 * Created by mbarreto on 6/21/15.
 */
class FormulaTest extends FlatSpec with Matchers {

  "A formula" should "be satisfiable when not all set" in {
    var formula = createFormula
    //not all literals set
    formula.isSatisfasiable(List(1,2)) should be (true)
    formula.isSatisfasiable(List(4,5)) should be (true)
  }

  it should "be satisfiable" in {
    var formula = createFormula

    formula.isSatisfasiable(List(1, 2, 3, -4, 5)) should be (true)
    formula.isSatisfasiable(List(1, -2, 3, 4, -5)) should be (true)
    formula.isSatisfasiable(List(1, -2, -3, 4, -5)) should be (true)
  }

  it should "not be satisfiable" in {
    var formula = createFormula

    formula.isSatisfasiable(List(-1, 2, -3, 4, 5)) should be (false)
    formula.isSatisfasiable(List(1, 2, 3, 4, -5)) should be (false)
    formula.isSatisfasiable(List(-1,-2,-3, 4, -5)) should be (false)
    formula.isSatisfasiable(List(1,2, 3, 4, -5)) should be (false)
    formula.isSatisfasiable(List(-1,2,-3, -4, -5)) should be (false)
    formula.isSatisfasiable(List(1,-2,-3, 4, 5)) should be (false)
    formula.isSatisfasiable(List(-1,-2,-3, 4, -5)) should be (false)
    formula.isSatisfasiable(List(-1,-3, 4)) should be (false)
    formula.isSatisfasiable(List(1, 2, 4)) should be (false)
  }






  private def createFormula : Formula = {
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
