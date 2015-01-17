package main.scala.domain

/**
 * Created by marbarfa on 3/2/14.
 */
class Clause {
  var id : Int = 0
  var literals : List[Int] = List[Int]()

  /**
   * Eg: literals = (1,-2,3), vars = (1,2,-6,10,-12)
   * vars.contains(1) = true => Clause is TRUE.
   * @return true if the clause is satisfiable.
   */
  def isSatisfasiable(vars : List[Int]) : Boolean = {
    var res = false
    var allLiteralsFound = true;
    for (l <- literals){
        if (vars.contains(l) || vars.contains(-l)){
          res = res || vars.contains(l);
        }else{
          allLiteralsFound = false;
        }
    }
    if (allLiteralsFound)
      return res;
    else
      return true;
  }

}
