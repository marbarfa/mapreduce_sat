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
      if (v.isValueSet){
        res = res || (((v.number > 0 ) && v.boolValue) || (v.number < 0 && !v.boolValue))
      }
    }
    return res
  }

}
