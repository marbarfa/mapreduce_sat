package algorithms

import algorithms.types.AlgorithmData
import domain.Formula
import org.scalatest.{Matchers, FlatSpec, FunSuite}
import utils.SatTestUtils

import scala.io.Source

/**
 * Created by mbarreto on 6/23/15.
 */
class UPPLEAlgorithmTest extends FlatSpec with Matchers{

  "A formula" should "be correctly reduced" in {
    var formula = SatTestUtils.createFormula

    var reduce1 = UPPLEAlgorithm.reduceFormula(formula, List(1))
    reduce1.clauses.size should be (4)
    reduce1.clauses.head.literals should be (List(2,-5, -4))
    reduce1.clauses.apply(1).literals should be (List(-2,-4))
    reduce1.clauses.apply(2).literals should be (List(-5, 3, -4))

    var reduce2 = UPPLEAlgorithm.reduceFormula(formula, List(-4))
    reduce2.clauses.size should be (1)
    reduce2.clauses.head.literals should be (List(3,5))

    var reduce3 = UPPLEAlgorithm.reduceFormula(formula, List(2, 5))
    reduce3.clauses.size should be (3)
    reduce3.clauses.apply(0).literals should be (List(1,3,-4))
    reduce3.clauses.apply(1).literals should be (List(-1, -4))
    reduce3.clauses.apply(2).literals should be (List(3, -4))
  }

  "A reduced formula" should "be as satisfasiable as the original" in {
    var formula = SatTestUtils.createFormula
    var formula2 = UPPLEAlgorithm.reduceFormula(formula, List(2, 5))

    formula2.clauses.apply(2).literals should be (List(3, -4))
    formula2.clauses.apply(2).isSatisfasiable(List(-3, 4)) should be (false)
    formula2.isSatisfasiable(List(-3,4)) should be (false)

    formula.isSatisfasiable(List(2,5)) should be (formula2.isSatisfasiable(List(2,5)))
    formula.isSatisfasiable(List(2,5,-3,4)) should be (formula2.isSatisfasiable(List(-3,4)))
    formula.isSatisfasiable(List(2, 5, -1, 4, -3)) should be (formula2.isSatisfasiable(List(-1, 4, -3)))
    formula.isSatisfasiable(List(1,4)) should not be (formula2.isSatisfasiable(List(1,4)))
    formula.isSatisfasiable(List(1,4, 2, 5)) should be (formula2.isSatisfasiable(List(1,4)))
  }

   it should "be equivalent after UPPLE algorithm" in {
    var formula = SatTestUtils.createFormula

     formula.clauses.apply(0).literals should be (List(1,3,-4))
     formula.clauses.apply(1).literals should be (List(2, -5, -4))
     formula.clauses.apply(2).literals should be (List(-1, -2, -4))
     formula.clauses.apply(3).literals should be (List(-5, 3, -4))
     formula.clauses.apply(4).literals should be (List(3, 4, 5))

     var upData = new AlgorithmData(List(-3, -5), formula)
     var (formula2, newFixed) = UPPLEAlgorithm.applyAlgorithm(upData)

     newFixed should be (List(-3, -5, 4, 1, -2))
     formula2.clauses.size should be (0)
  }

  it should "be equivalent after UPPLE of the CNF" in {
    var formula = new Formula()
    formula.doReadCNF(Source.fromURL(getClass.getResource("/3sat_10_20.txt")).getLines());

    var upData = new AlgorithmData(List(3, 2), formula)
    var (formula2, newFixed) = UPPLEAlgorithm.applyAlgorithm(upData)

    newFixed should be (List(3, 2, -8))
    formula.clauses.size should be (12)
    formula2.clauses.size should be (6)

    formula2.clauses.apply(5).literals should be (List(-10, -9, 7))
    formula2.clauses.apply(4).literals should be (List( 9, 7))
    formula2.clauses.apply(3).literals should be (List(10, 9, 1))
    formula2.clauses.apply(2).literals should be (List( -4, -10))
    formula2.clauses.apply(1).literals should be (List(5, -1))
    formula2.clauses.apply(0).literals should be (List(-7, -6, 5))

    var upData2 = new AlgorithmData(List(-5, -9), formula2)
    var (formula3, newFixed2) = UPPLEAlgorithm.applyAlgorithm(upData2)

    newFixed2 should be (List(-5, -9, -1, 10, -4, 7, -6))
    formula3.clauses.size should be (0)
  }
}
