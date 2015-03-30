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
    //iterate over all literals of the clause
    for (l <- literals){
      //if vars contains the literal (in its true or false state) => true
      if (vars.contains(l)){
        return true;
      }else if (!vars.contains(-l)){
        //if vars not contains l nor -l, then the literal is not present in the fixed set=> treat it as 'x'
        //so..., the clause could be true yet.
        return true;
      }
    }
    return false
  }

}
