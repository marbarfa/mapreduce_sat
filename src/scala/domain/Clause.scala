package scala.domain

/**
 * Created by marbarfa on 3/2/14.
 */
class Clause {

  var variables : List[Variable] = _

  /**
   * @return true if the clause is satisfiable.
   */
  def isSatisfasiable() : Boolean = {
    var res = false
    for (v <- variables){
      res = res || (v.number > 0)
    }
    return res
  }

}
