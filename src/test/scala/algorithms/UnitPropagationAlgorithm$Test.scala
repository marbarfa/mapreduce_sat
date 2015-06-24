package algorithms

import algorithms.types.AlgorithmData
import domain.Clause
import org.scalatest.{Matchers, FlatSpec, FunSuite}
import utils.SatTestUtils

/**
 * Created by mbarreto on 6/22/15.
 */
class UnitPropagationAlgorithmTest extends FlatSpec with Matchers {

  "A formula with a literal set" should "be correctly propagated" in {
    var formula = SatTestUtils.createFormula
    var newClause = new Clause();
    newClause.literals = List(5)
    formula.clauses ++= List(newClause)
    formula.addClauseOfVar(5, newClause)

    var dfsData = new AlgorithmData(List(1,2), formula)

    var (formulaRes, newFixed) = UnitPropagationAlgorithm.applyAlgorithm(dfsData)
    newFixed should be (List(1,2,-4))
    newFixed.contains(-4) should be (true)

    formulaRes.containsClause(List(1, 3, -4)) should be (false)
    formulaRes.containsClause(List(2, 5, -4)) should be (false)
    formulaRes.containsClause(List(-1, -2, -4)) should be (false)
    formulaRes.containsClause(List(3, 4, 5)) should be (true)

  }
}
