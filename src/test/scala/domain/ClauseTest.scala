package domain

import org.scalatest._

/**
 * Created by mbarreto on 6/21/15.
 */
class ClauseTest extends FlatSpec with Matchers {

  "A clause" should "be not satisfasiable" in {
    var clause = new Clause()
    clause.literals = List(1, -2, 3)

    var possible1 = List(-1, 2, 5, -3);
    var possible2 = List(-1);
    var possible3 = List(2);
    var possible4 = List(3);

    clause.isSatisfasiable(possible1) should be (false)
    clause.isSatisfasiable(possible2) should be (true)
    clause.isSatisfasiable(possible3) should be (true)
    clause.isSatisfasiable(possible4) should be (true)

    clause.isSatisfasiable(List[Int]()) should be (true)
  }

}
