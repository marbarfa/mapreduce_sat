package main.scala.utils

/**
 * Created by marbarfa on 3/9/14.
 */
trait ConvertionHelper {

  implicit def bool2int(b: Boolean): Int = if (b) 1 else 0;

  implicit def int2bool(b: Int): Boolean = if (b == 1) true else false;

  /**
   * Example: given Map[1:true,2:false,3:false,4:true] => 1 -2 -3 4
   * @param literals
   * @return
   **/
  def literalMapToDBKey(literals: Set[Int]): String =
    literals
      .toList
      .sorted.foldLeft("") {
        (acc, k) => acc +"_"+ k
    }

}
