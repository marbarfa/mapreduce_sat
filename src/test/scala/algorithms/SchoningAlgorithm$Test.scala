package algorithms

import domain.Clause
import org.scalatest.{Matchers, FlatSpec, FunSuite}

/**
 * Created by mbarreto on 6/22/15.
 */
class SchoningAlgorithm$Test extends FlatSpec with Matchers {


  "A false clause" should "correctly flip one of its literals" in {
    var clause = new Clause();
    clause.literals = List(1,-2,3)

    SchoningAlgorithm.flipLiteralOfClause(clause, List(-1,2,-3), 0) should be (List(1,2,-3))
    SchoningAlgorithm.flipLiteralOfClause(clause, List(-1,2,-3), 1) should be (List(-1,-2,-3))
    SchoningAlgorithm.flipLiteralOfClause(clause, List(-1,2,-3), 2) should be (List(-1,2,3))
  }

}
