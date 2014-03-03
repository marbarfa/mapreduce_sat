package scala.domain

/**
 * Created by marbarfa on 3/2/14.
 */
class Formula {

  var clauses : List[Clause] = _

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

}
