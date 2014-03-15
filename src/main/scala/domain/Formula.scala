package main.scala.domain

import scala.collection.immutable.HashMap

/**
 * Created by marbarfa on 3/2/14.
 */
class Formula {

  def getFalseClauses(literals: Map[Int, Boolean]): List[Clause] = {
    var falseClauses = List[Clause]();

    literals.keySet.foreach(literal => {
      clausesOfVars
        .getOrElse(literal, List[Clause]())
        .foreach(clause => {
          if (!clause.isSatisfasiable(literals)) {
            falseClauses ::= clause
          }
        })
    })

    return falseClauses;
  }


  var clauses: List[Clause] = _
  var clausesOfVars: Map[Int, List[Clause]] = new HashMap[Int, List[Clause]]()

  var n: Int = _
  var m: Int = _

  /**
   * @return true if the formula is satisfiable
   */
  def isSatisfasiable(literals: Map[Int, Boolean]): Boolean = {
    var res = true
    for (c <- clauses) {
      res = res && c.isSatisfasiable(literals)
    }
    return res
  }

  /**
   * Adds a new dependable clause to a literal @literal.
   * This method is used to have a map of literal ==> where it appears in the Formula.
   * @param literal
   * @param clause
   */
  def addClauseOfVar(literal: Int, clause: Clause) {
    clausesOfVars += (literal -> (clause :: clausesOfVars.getOrElse(literal, List[Clause]())))
  }


}
