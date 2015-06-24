package domain

import java.io.{FileInputStream, File, DataInputStream}

import org.scalatest._
import utils.SatTestUtils

import scala.io.Source

/**
 * Created by mbarreto on 6/21/15.
 */
class FormulaTest extends FlatSpec with Matchers {

  "A formula" should "be satisfiable when not all set" in {
    var formula = SatTestUtils.createFormula
    //not all literals set
    formula.isSatisfasiable(List(1,2)) should be (true)
    formula.isSatisfasiable(List(4,5)) should be (true)
  }

  it should "be satisfiable" in {
    var formula = SatTestUtils.createFormula

    formula.isSatisfasiable(List(1, 2, 3, -4, 5)) should be (true)
    formula.isSatisfasiable(List(1, -2, 3, 4, -5)) should be (true)
    formula.isSatisfasiable(List(1, -2, -3, 4, -5)) should be (true)
  }

  it should "not be satisfiable" in {
    var formula = SatTestUtils.createFormula

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

  it should "read a CNF formula correctly" in {
    var formula = new Formula()
    formula.doReadCNF(Source.fromURL(getClass.getResource("/3sat_30_134.txt")).getLines());
    formula.n should be (30)
    formula.m should be (134)
    formula.getFalseClause(List(-27,29, -1, 26, 4, -21, 2, 3, -5, 6, 7, -8, -9, -10, -11, 12, 13, 14, -15, 16, -17, 20, 25, 23, -22, -28, 30, 18, -19,
      -24)) should be (List(-23,-20,21))

    formula.getFalseClause(List(-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12, -13, -14,-15, -16, -17, -18, -19, -20, -21, -22,
      -23, -24, -25, -26, -27, -28, -29, -30)) should be (List(1,2,18))

    formula.isSatisfasiable(List(-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12, -13, -14,-15, -16, -17, -18, -19, -20, -21, -22,
      -23, -24, -25, -26, -27, -28, -29, -30)) should be (false)
  }


  it should "return the correct list of clauses" in {
    var formula = SatTestUtils.createFormula
    var clauses = formula.getClauses(List(5))
    clauses.size should be (3)
    clauses.head.literals should be (List(3,4,5))
    clauses.apply(1).literals should be (List(-5, 3, -4))
    clauses.apply(2).literals should be (List(2,-5,-4))
  }


}
