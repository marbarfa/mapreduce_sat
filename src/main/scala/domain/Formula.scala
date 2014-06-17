package main.scala.domain

import org.apache.log4j.Logger
import scala.collection.immutable.HashMap

/**
 * Created by marbarfa on 3/2/14.
 */
class Formula {

  var clauses: List[Clause] = List[Clause]()
  var clausesOfVars: Map[Int, List[Clause]] = new HashMap[Int, List[Clause]]()

  var n: Int = _
  var m: Int = _

  var literalsInOrder : List[Int] = null;



  /**
   * @return true if the formula is satisfiable
   */
  def isSatisfasiable(literals: List[Int], log : Logger): Boolean = {
    var res = true
    var affectedClauses = getClauses(literals, log)
    for (c <- affectedClauses if res) {
      res = res && c.isSatisfasiable(literals)
    }
    return res
  }

  def getClauses(literals : List[Int], log: Logger) : List[Clause] = {
    var clauses = literals.foldLeft(List[Clause]()) {(list, key) => list ::: clausesOfVars.getOrElse(math.abs(key), List[Clause]())}
    return clauses;
  }


  /**
   * Adds a new dependable clause to a literal @literal.
   * This method is used to have a map of literal ==> where it appears in the Formula.
   * @param literal
   * @param clause
   */
  def addClauseOfVar(literal: Int, clause: Clause) {
    var literalAbs = math.abs(literal);
    clausesOfVars += (literalAbs -> (clause :: clausesOfVars.getOrElse(literalAbs, List[Clause]())))
  }

  def getFalseClauses(literals: List[Int]): List[Clause] = {
    var falseClauses = List[Clause]();

    literals.foreach(literal => {
      clausesOfVars
        .getOrElse(math.abs(literal), List[Clause]())
        .foreach(clause => {
        if (!falseClauses.contains(clause) &&
            !clause.isSatisfasiable(literals)) {
          falseClauses ::= clause
        }
      })
    })

    return falseClauses;
  }

  /**
   * Order clausesOfVars by how many clauses are related to each literal
   * @return
   */
  def getLiteralsInOrder() : List[Int] = {
    if (literalsInOrder == null){
      literalsInOrder = clausesOfVars.toSeq.sortBy(_._2.size).reverse.toList map (x=> x._1)
    }
    return literalsInOrder;
  }

}
