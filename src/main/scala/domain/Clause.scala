package main.scala.domain

/**
 * Created by marbarfa on 3/2/14.
 */
class Clause {

  var literals : List[Int] = List[Int]()

  /**
   * @return true if the clause is satisfiable.
   */
  def isSatisfasiable(vars : Map[Int, Boolean]) : Boolean = {
    var res = false
    for (l <- literals){
        res = res || getBoolValue(l, vars)
    }
    return res
  }
  
  def getBoolValue(l: Int, vars : Map[Int, Boolean]) : Boolean = {
    if (vars.contains(math.abs(l)))
      return ((l>0) && vars(l)) || ((l<0) && vars(l))
    return true;  
  }

}
