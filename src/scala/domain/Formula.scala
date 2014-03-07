package scala.domain

/**
 * Created by marbarfa on 3/2/14.
 */
class Formula {

  var clauses : List[Clause] = _
  var clausesOfVars : Map[Int, List[Clause]] = new Map[Int, List[Clause]]()

  var n : Int = _
  var m : Int = _
  /**
   * @return true if the formula is satisfiable
   */
  def isSatisfasiable() : Boolean = {
    var res = true
    for (c <- clauses){
      res = res && c.isSatisfasiable()
    }
    return res
  }
  
  def addClauseOfVar(literal : Int, clause : List[Clause]) {
      clauses.put(literal, clause :: if (!clauses.containsKey(literal)) List[Clause]() else clauses.get(literal))
  }
  

}
